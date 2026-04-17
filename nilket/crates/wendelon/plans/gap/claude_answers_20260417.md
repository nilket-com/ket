# Lead Architect Answers — Open Questions
**Date:** 2026-04-17
**Derived from:** `plans/gap/open_questions_20260417.md`
**Stance:** god-tier software perfection, not pragmatism

Every answer aims at the correct long-run shape, not the cheapest next step. Where pragmatism and perfection diverge, I pick perfection. Questions I cannot answer with conviction are marked *Unresolved*.

---

## 1. Integration Gate & Bring-Up

### Test topology — separate processes or in-process?
Both. The harness owns a scenario DSL (`form_cluster`, `write`, `read`, `partition`, `crash`, `restart`) and two executors: in-process (real `quinn::Endpoint`s in one test binary) and multi-process (spawn stock daemons, wire them by config). Same scenarios, two executions. In-process runs on every PR for speed and debuggability; multi-process runs nightly and on release candidates to cover signal handling, config file loading, stderr, and `sd_notify`. A scenario that only passes in one mode is a bug to investigate, not a choice to make.

### Parallel CI port allocation
Never bind to fixed ports. Every listener binds to `:0`; the OS assigns the port, and the harness reads it back from the bound socket. Peer discovery goes through an in-harness registry (or a per-test filesystem rendezvous file for multi-process). Environments that need to reach external QUIC endpoints (e.g., nightly chaos runners) use per-job network namespaces. Port conflicts cease to be possible by construction.

### Test certificate strategy
`rcgen` at runtime, every test, no exceptions. Each test generates a fresh ephemeral CA, fresh per-node keypairs with correct SANs, and short not-before/not-after windows. Checked-in fixtures calcify, leak into production by accident, and prevent exercising the cert-issuance code paths the test is supposed to cover. The dev dependency is negligible; the correctness coverage win (you are constantly testing cert parsing and rotation) is substantial.

### L2 config loading timing
Land L2 config parsing in the bring-up workstream. The TOML schema must be complete from day one; adding sections later forces a migration. Parsing is not enabling — the section is optional and leaving it absent keeps tiering off. A node upgraded from version 1 reads the same schema as version N.

### Minimum bring-up observability
Full `tracing-subscriber` layer stack from day one: `fmt` layer (pretty for TTY, JSON otherwise) + `EnvFilter` with a reload handle + an OTLP layer slot initially bound to a no-op exporter. Reload handle exists so operators can change filter verbosity without restarting. You never want to rip out a logging bootstrap after production. The OTLP slot being no-op at bring-up time is cheap; having no slot forces a retrofit later.

---

## 2. Immediate Correctness & Safety Fixes

### Concurrency controls — transport cap or broader framework?
Both, at different layers. Transport-layer per-peer stream semaphore in the QUIC accept loop is non-negotiable (default 256 concurrent streams per peer, configurable) and lands immediately — it is a local defense against fan-out misbehavior and is independent of whatever admission control comes later. The broader admission-control framework answers a different question (global memory/work budget) and lands with the resource-control workstream. Neither blocks the other.

### Deterministic dev secrets
Stop emitting them. `render_example_config` emits placeholder tokens (e.g., `__REPLACE_WITH_GENERATED_SIGNING_KEY__`) and the daemon refuses to start while placeholders are present. A sibling command — `wendelon-ctl init-secrets --out <dir>` — generates fresh signing material on demand. Warnings fail because operators don't read warnings; refusing to start is the only reliable forcing function. Dev-only test builds bypass the check behind a feature flag explicitly named `insecure-deterministic-keys`.

### Scope of the rapid-fix phase
If a symptom is both severe and has a bounded short-term fix, the short-term fix lands in the rapid-fix tranche and is retired when the long-term design ships. Unbounded growth paths and silent data loss never wait. Specifically: pending-upload tmp-file leaks, the follower disk leak, and the idempotency cache TTL all land now even though the authoritative designs come later. If later designs subsume the short-term fixes, we delete the short-term code without regret.

---

## 3. Raft Log, Checkpoint, and Snapshot Lifecycle

### WAL compaction strategy
Segmented WAL with file rotation. The WAL is a sequence of fixed-size segment files (e.g., 128 MiB) named by their starting log index. Compaction is drop-segments-below-snapshot, not hole-punch, not rewrite. Reasons: (a) hole-punch interacts badly with O_DIRECT on ext4 and is inconsistent across filesystems; (b) rewrite doubles write amplification and couples compaction to state; (c) segment rotation aligns naturally with per-segment DEKs for encryption rotation; (d) cold segments can be archived off-host without rewriting anything. This is the model every serious log-structured system converges to.

### openraft ownership boundary
openraft owns consensus sequencing; Wendelon owns durability and on-disk layout. openraft's `RaftLogStorage::purge`, snapshot install, and snapshot build callbacks are integration points — we implement them correctly, fsync before returning, and never acknowledge until media durability is actually committed. We do not fight openraft's invariants; we honor every callback contract. This is the only correct division: consensus framework decides *when*, storage decides *how*, storage guarantees *durability-before-ack*.

### Snapshot trigger policy
Both log count and time. Either threshold fires a snapshot; whichever hits first. Log count bounds worst-case recovery replay time; time bounds the case of an idle-but-live node whose log accumulates slowly. Both thresholds are TOML-configurable with production defaults.

### Snapshot thresholds
Tune so that worst-case replay from the most recent snapshot is bounded to ~30s on reference hardware. For a control-plane-heavy state machine (each entry is a membership/lease/epoch event, not a data write), expect ~50k–100k entries between snapshots, or a 6h ceiling. Publish the reference-hardware benchmark alongside the defaults so operators can reason about their own numbers.

### Checkpoint timing
Both. Checkpoints are emitted at every snapshot boundary (always) *and* periodically (every 1000 applied entries or 5s, whichever first). Periodic checkpoints bound recovery work even when snapshots are rare; boundary-tied checkpoints give snapshot installs a logical commit point.

### Snapshot transport and layout
Fixed-size chunks (4 MiB, page-aligned for O_DIRECT), streamed, with a manifest chunk at the end referencing each data chunk by hash. Semantic boundaries are an anti-pattern — they couple the snapshot wire format to state machine schema, which forces wire-format migrations every time the state machine evolves. Chunking is pilot-mandatory: 10 GB state machines are plausible from day one in enterprise deployments; whole-buffer install would OOM the node.

### Snapshot compression
zstd, with dictionary training on representative state-machine snapshots. Level 3 for live snapshot build (CPU budget matters during normal operation); level 9 for background re-compression of archived snapshots. Dictionary training gives 3–5× better ratios than lz4 on structured Raft state, and zstd decode is fast enough not to matter. Compression happens *above* encryption — encrypted ciphertext is incompressible.

---

## 4. Tiered Object Storage (L2)

### Range-read priority
Required for first production L2 integration. A point read that downloads 64 MiB to return 128 bytes is operationally unusable — latency, egress cost, and cache churn are all prohibitive. Shipping L2 without range reads is shipping broken L2.

### ObjectStore breaking-change scope
Land the full trait redesign in one coordinated change: `get`, `get_range`, `put`, `put_multipart`, `delete`, `list`, `head`, `copy`. The trait is small, internal consumers are few, and serializing the breaking changes just extends the churn. Design the canonical shape once and migrate all call sites once.

### Range-read index placement
Footer-embedded record offset index. Slab layout: `[magic | version_header | record_0 | record_1 | ... | record_n | offset_index | footer_checksum | index_pointer]`. The reader issues one small range-GET for the tail (64 KiB) to read the pointer + footer + index, then one targeted range-GET for the record. One S3 object per slab. This is the Parquet/ORC model and is the proven answer. Sidecar manifests double object count and introduce consistency puzzles; S3 user metadata is tightly size-limited.

### Pending-upload recovery model
Raft-managed state, not a local journal. The upload lifecycle (`Pending → Uploaded → Committed → LocalPurged`) is modeled as explicit state machine transitions in the Raft log. Recovery follows Raft invariants — one source of truth. A local journal invents a second recovery story that must be reconciled with Raft; two sources of truth is zero sources of truth.

### Orphan cleanup authority
Leader-only. Destructive operations require the freshest state, which only the leader has authoritatively. The reconciler pages the bucket's LIST output, joins against `tiered_slabs` from the committed state machine, and deletes objects not present in state. Rate-limited, with a configurable max-deletes-per-minute. Followers never delete remote objects.

### Multipart threshold
Align with `slab_max_bytes`. If slabs are 64 MiB, multipart kicks in at 64 MiB. Provider-default thresholds are noise; our own slab sizing is the authoritative configuration.

### Remote reclaim scope
Full lifecycle in this workstream: migration cleanup, retention expiry, tombstone reclamation, orphan GC. Defer only archival tiering (Glacier-class, cross-region replication, lifecycle policies managed by the provider). Tiering without deletion is a cost bomb and violates data-retention contracts.

---

## 5. Encryption, Key Management, and L2 Crypto

### Standard cipher
AES-256-GCM-SIV. Plain AES-GCM has a 96-bit nonce and nonce-misuse is catastrophic; at billions-of-records scale the collision analysis is nontrivial. GCM-SIV is misuse-resistant — nonce reuse degrades to a distinguisher rather than a key recovery. On AES-NI hardware GCM-SIV is 2–3× faster than XChaCha20-Poly1305. XChaCha20 is an acceptable fallback if aws-lc-rs FIPS does not yet ship GCM-SIV at implementation time (confirm during ADR).

### Encryption granularity
Per-slab DEK with intra-slab 64 KiB chunked encryption. Each slab gets its own DEK on creation. Within the slab, plaintext is split into fixed 64 KiB chunks and each chunk is encrypted independently with a deterministic nonce (`slab_id || chunk_index`). This gives: (a) range-GET decryption without decrypting the whole slab; (b) bounded nonce space per DEK; (c) clean rotation story — a new slab picks a new DEK. Per-record is too much nonce management; per-whole-slab prevents range reads.

### Alignment and performance model
Encryption sits *above* `AlignedBuf`. Plaintext encrypts into a ciphertext chunk of exact size (plaintext + 16B tag + 12B nonce), then the ciphertext stream is padded to the next 4 KiB boundary for O_DIRECT. Padding overhead is <0.1% on slab-sized writes. Encrypting below the alignment layer corrupts page-alignment invariants and defeats the purpose of `AlignedBuf`.

### L2 encryption mode
Client-side envelope encryption only. SSE-KMS surrenders trust to the cloud provider, breaks cross-cloud portability, and couples key lifecycle to provider APIs. Wendelon's posture is that the cloud sees only ciphertext and wrapped DEKs. SSE-KMS support is an anti-feature and is not shipped.

### DEK caching policy
15-minute TTL, LRU eviction under memory pressure, zeroize-on-drop (`zeroize` crate with `ZeroizeOnDrop`). Cache size bounded by the global memory budget (see §9). On any explicit key-rotation event, the affected entries are invalidated immediately.

### FIPS compatibility
aws-lc-rs is the FIPS backend. Verify at ADR time that it ships AES-256-GCM-SIV; if not, fall back to AES-256-GCM with record-id-derived counter nonces and publish the nonce-collision analysis in the ADR. Never hand-roll crypto.

### Migration scope
In scope. A background re-encryption sweeper reads plaintext slabs, encrypts via the normal slab-creation path, and retires the old slab through the existing compaction machinery. Ships alongside the tiering work because the interaction matters — plaintext slabs promoted to L2 must be encrypted on write, not after. Without a migration story, operators enable encryption only on new clusters, which is useless for existing deployments.

---

## 6. Authorization, Audit, and Compliance

### Authorization mechanism
Certificate OID extension encoding role and scopes. The extension carries a CBOR (not JSON — binary framing matters in X.509) payload: `{"role": "operator", "scopes": [...], "tenant_id": "...", "key_id": "..."}`. mTLS authenticates; the OID payload authorizes; one credential root. Separate CAs double cert management without adding authority beyond role-switching, which the OID already handles. Tokens introduce a new credential type with its own rotation and bearer-token risk. Cert-embedded claims are atomic, revocable (via CRL/OCSP or short-lived certs), and aligned with the existing mTLS trust model.

### Pilot authorization scope
Three roles: `data_node`, `operator`, `read_only_observer`.
- `data_node`: replicate, vote, serve client reads/writes. No admin RPCs.
- `operator`: all of `data_node` plus membership changes, epoch advances, lease grants, transfer leadership, drain, backup/restore.
- `read_only_observer`: read-only access to cluster status, metrics, audit tail. No state-mutating RPCs.

This triad covers every enterprise deployment I have seen; extra roles are added later via the same OID mechanism without a protocol break.

### Audit sink model
Structured event stream abstraction from day one. `AuditSink` trait with three initial implementations: `LocalNdjson` (default), `Syslog` (Unix socket), and `Kafka` (behind feature flag for SIEM-shipping deployments). The trait makes call sites sink-agnostic; adding new sinks is zero-churn. Shipping only a local file first forces every later integration to touch every call site.

### Audit tamper evidence
Hash chain from day one. Each record carries `prev_hash = blake3(previous_record_canonical_bytes)`. The chain head is exposed via a dedicated control RPC so auditors can snapshot the head and detect gaps or rewrites. On every Raft-committed audit event, the hash chain extends. Non-negotiable for regulated-industry compliance; cheap to implement upfront and expensive to retrofit.

### FIPS target
Use of a FIPS-validated crypto module is the deliverable for this workstream. Full FIPS 140-3 certification of the Wendelon binary is tracked as a separate multi-month lab program. The code must be FIPS-ready (aws-lc-rs backend, no non-validated primitives, no unused crypto code paths in the FIPS build) so certification is unblocked when the program starts.

### Rate limiting identity
Per authenticated identity (node_id/tenant_id from cert) for authenticated connections; per source IP for pre-auth traffic (handshake failures, malformed frames). Composition: a connection first hits per-IP counters; once mTLS succeeds, it graduates to per-identity counters. Cert identity is authoritative — per-IP alone fails when legitimate traffic is NATed and doesn't help against authenticated adversaries; per-identity alone can't defend the handshake.

---

## 7. Observability, CLI, and Operator Surface

### Trace export protocol
OTLP only. OTLP is the industry convergence point in 2026. Jaeger, Tempo, Honeycomb, Datadog, New Relic, and the OpenTelemetry Collector all ingest OTLP. Shipping a second native protocol is maintenance tax with no user benefit.

### CLI packaging
Separate binary: `wendelon-ctl`. Operators install it on bastions or laptops, not on data nodes. Different release cadence, different attack surface, smaller footprint, and clear client/server separation. Bundling into the daemon couples the operator interface to server restarts.

### CLI minimum surface (pilot-mandatory)
`cluster-status`, `transfer-leader`, `drain`, `backup`, `restore`, `node-join`, `node-remove`, `audit-tail`, `config-dump`. This set is the minimum below which an operator cannot run an incident. Stretch goals (scale, restore-to-point-in-time, key-rotate, tenant-admin) ship in the same binary as they become available.

### Health endpoint security
Split the endpoint. `/health` (liveness) is unauthenticated, returns only liveness — process alive, not crashed. No information disclosure. k8s liveness probe and load balancer health checks hit `/health`. `/ready` requires mTLS and returns detailed state (cluster member list, leader, epoch, drain status). k8s readiness probe hits `/ready` via the service mesh that does mTLS for it. Two endpoints, two security postures, matched to their use cases.

### Health versus metrics listeners
Separate listeners on separate ports. Health must be reachable by unauthenticated probes; metrics must be reachable only by authenticated scrapers. Sharing forces the weaker posture on both. The cost of a second listener is negligible.

### Metrics hardening boundary
Secure metrics exposure lands fully in the observability workstream, not partially in security with a later finish. Partial hardening lets an insecure default persist in deployments during the gap window, and removing an insecure default is harder than introducing a secure one.

### Observability depth for pilot
All of: distributed tracing with context propagation across QUIC RPCs, histogram-based latency metrics with dimensional labels, OTLP export, and scraped metrics with OpenMetrics format. An operator who cannot trace a slow request across nodes cannot operate the cluster. Deferring any of these delays the day operators can diagnose their own problems.

---

## 8. Fault Injection and Deterministic Correctness Testing

### Harness shape
Both, in sequence. Start with a process-level chaos harness (real daemons, real QUIC, controlled partitions/crashes via `nftables` and `SIGKILL`) because it exercises the product path immediately without refactoring io_uring. Add a deterministic simulator once the storage layer has been abstracted behind an `AsyncFileOps` trait. Both surface different bug classes; keep both permanently.

### Simulator choice
turmoil-style for the network layer (Tokio-native, QUIC-friendly) combined with a custom storage simulator. madsim's tokio integration is elegant but its io_uring story is weak and its reach into raw-socket behavior is not what we need. A purpose-built storage simulator gives precise disk fault injection (slow writes, torn writes, ENOSPC, bit-flips, reordered completions) that no off-the-shelf framework matches.

### Storage-path abstraction
Yes. Extract an `AsyncFileOps` trait (or equivalently `StorageBackend`) that the production path implements via io_uring and the simulator implements via an in-memory model with injectable faults. The production implementation is a thin wrapper — the abstraction does not hide io_uring's semantics; it exposes them through a trait surface. This is a multi-week refactor and is worth every day of it: once done, every subsequent storage bug can be reproduced deterministically with a seed.

### Tiering/S3 simulation
Mock `ObjectStore` trait implementation. The trait is already the right abstraction boundary. A fault-injecting mock (configurable latency, 500/503 errors, partial failures, eventual consistency windows) is faster, more deterministic, and easier to maintain than LocalStack or a real S3 simulator.

### Correctness checker strategy
Elle for representative subset; custom monotonic-counter checker for broader coverage. Elle (Jepsen's successor to Knossos) gives real linearizability analysis across history. Its cost is per-operation nontrivial, so run it against a focused operation set (write + read on a small key space). The monotonic-counter checker is cheap and catches a broad class of violations; run it over everything.

### Coverage targets
- CI per-PR: 10,000 logical operations per scenario, ~1M total across scenarios.
- Nightly: 1M operations per scenario, random seeds, no fault budget cap.
- Weekly: 100M-operation soak, random workloads, random fault timings.

Each failure prints its seed; every failure becomes a permanent regression scenario with the seed frozen.

---

## 9. Durability, Repair, and Resource Control

### Durability model
Explicit fsync at consensus-ack boundaries. Do not trust PLP NVMe. Reasons: (a) fleets are heterogeneous; (b) PLP guarantees vary across vendors and firmware revisions; (c) Linux O_DIRECT does not promise media durability without fsync regardless of hardware; (d) the cost of fsync is amortized by group commit. "Trust the hardware" is a false economy that trades correctness for throughput the group-commit coalescer already recovers.

### Group-commit tuning
Adaptive window with a floor and ceiling. Floor 200µs (low load — prioritize latency). Ceiling 2ms (p99 cap). Between them, window scales with current inflight-write rate so the coalescer expands naturally under load and collapses naturally when idle. Exact formula tuned by benchmarks, but the shape is fixed: `window = clamp(inflight × per_write_cost, 200µs, 2ms)`.

### Persistent local index
Custom, purpose-built format, checkpointed atomically alongside WAL fsync. The index is small and domain-specific — pulling in redb or sled adds a dependency, a storage format we don't own, and a tuning surface we don't need. A purpose-built format matches our encryption story, our alignment story, and our recovery story. Fallback to full-scan on index corruption (loud logs, metric alert).

### Repair automation
Automatic repair from healthy replicas. When the scrubber finds corruption and at least one replica's bytes match a quorum-agreed checksum, the corrupt copy is overwritten automatically. Manual escalation only if no healthy source exists. Rate-limited, I/O-bounded, cryptographically verified before any destructive write. Manual repair in a distributed system is an SRE-hours tax nobody pays.

### Memory budget enforcement
Semaphore-based byte-permits. A global `Semaphore` issues permits measured in bytes; each allocation path acquires permits before allocating and releases on drop (RAII guard). Semaphores give natural queuing (backpressure is free), natural fairness, and avoid the check-then-allocate race that atomic counters have. Per-subsystem sub-semaphores derive from the global pool.

### Admission-control breadth
Ship the full admission-control layer in one phase, config-flag-gated (`admission.enabled`, default true). Operators tune thresholds; the mechanism lands complete. Partial admission control leaves unclosed overload paths indefinitely — the whole point of admission is that *every* ingress path is governed.

### Buffer-path priorities (pilot blockers)
Streaming versions of:
1. Cold L2 reads (no whole-slab buffering on point read).
2. Snapshot install/build (chunked, not `Cursor<Vec<u8>>`).
3. Arrow query materialization (batch-streaming, not collect-into-vec).

All three are pilot blockers because each can OOM the node under modest enterprise workload. Anything not on this list is a performance follow-up.

---

## 10. Multi-Tenancy and Tenant-Aware Policy

### Pilot requirement
Defer multi-tenancy to post-pilot. Multi-tenancy is a cross-cutting design whose right shape is informed by real-world feedback from the single-tenant pilot. Shipping MT before pilot risks burning the design budget on the wrong abstraction.

### Namespace model
Separate keyspace per tenant, not shared keyspace with prefixes. Independent compaction, independent ring placement, independent encryption keys, clean offboarding (drop the keyspace, the encryption key, and move on). Prefix sharing couples tenants operationally — one tenant's compaction or hot-key pressure leaks into another's performance envelope, which is exactly what tenant isolation must prevent.

### Tenant identity propagation
From authenticated client cert material via the OID extension (§6). Not request headers (trivially spoofable), not URL-prefix routing (middleware-bypass risk), not per-connection authenticated headers (one more credential type). The cryptographic identity root is the *only* authoritative binding.

### Policy enforcement boundary
Both layers, different responsibilities:
- Data path enforces instantaneous limits: rate limits, admission, per-request authorization.
- Raft state machine enforces durable limits: storage quota, key isolation, tenant lifecycle state.

Data-path-only lets a race defeat durable quotas. State-machine-only adds consensus latency to every ingress. Both exist because both failure modes exist.

### Quota enforcement
Both. Data-server does admission-time soft-rejection against a cached quota view (saves the consensus round-trip in the common case). State machine does final authoritative accounting; a periodic reconciliation sweep catches drift between cached and authoritative counters. Clear error on hard-rejection; never OOM.

### Tenant lifecycle
Control RPCs via a dedicated `TenantAdmin` interface, every operation a Raft-committed state machine transition with audit log entries. Not config-file-driven (config reloads are eventually consistent; tenant state is authoritative). Not external-control-plane-only (locks operators into a specific deployment topology). RPC is the right shape because tenant state is first-class and auditable.

### Existing plan alignment
Treat `plans/multi_tenancy.md` as prior art. Write the new ADR from scratch, cite the prior doc where it agrees, explicitly supersede it where this ADR disagrees, and record rationale per deviation. Do not silently diverge.

### Per-tenant topology and isolation
Yes, and staged:
- Phase 1: per-tenant replication factor. Smallest change, affects only placement.
- Phase 2: per-tenant L2 bucket routing. Affects storage lifecycle.
- Phase 3: per-tenant KEK isolation. Affects crypto.

Shipping together couples three large changes; staging lets each be validated independently and rolled back independently.

### KMS fan-out cost
Model: cost per I/O batch = (distinct tenants in batch × KMS unwrap p99) ÷ batch amortization factor, with DEK cache providing amortization on repeat access. Worst case (one tenant per record, cold cache) is arbitrarily bad; realistic case (one tenant per batch, warm cache) is negligible. Mitigation is structural: batch writes *by tenant* at ingress so each batch's crypto cost is one unwrap, not N. Same pattern for reads under the DEK cache. Publish the cost formula in operator docs so cluster sizing is informed.

---

## Unresolved

- **Exact group-commit window constants** (§9 adaptive window). The shape is settled; the floor/ceiling/scaling constants need benchmark data against real target hardware. Not guessing — measuring.
- **aws-lc-rs FIPS coverage of AES-256-GCM-SIV** (§5, §6). Depends on the library version at ADR time. The fallback path (plain AES-GCM with carefully-derived counter nonces) is known-good, but I won't pre-commit without confirming the library surface.
- **Concrete snapshot compression ratio on real state machine data** (§3). The dictionary-trained zstd claim rests on generic structured-data benchmarks; a measurement on representative Wendelon state is still needed to confirm the multiplier.
- **io_uring abstraction refactor cost** (§8). The shape of `AsyncFileOps` is clear in principle; the actual code surface inside `src/storage/io_uring.rs` and its callers determines whether the refactor is 2 weeks or 6. Needs a prototype before committing.
