# Wendelon Gap Analysis — Fortune 500 Readiness Review

**Date:** 2026-04-17
**Reviewer perspective:** Principal Database Architect

## Executive summary

Wendelon has undergone an intense ~48-hour push (30+ commits between the
2026-04-15 gap assessment and today) that closes most of the architectural
gaps the prior `plans/gap/*` analyses flagged. The internals are substantive:
~25k LOC of Rust, ~7.2k LOC of tests, and the correctness-shaped gaps
(fsync-before-quorum, WAL recovery, TLS SAN binding, Control Ring RPCs,
lease Ed25519, migration automation) are now genuinely implemented — not
stubbed.

However, the prior assessments understated five categories that a Fortune
500 data warehouse deployment would fail on immediately: encryption at rest
is entirely absent, the production binary (`main.rs`) cannot actually bring
up a multi-node TLS cluster, not a single integration test exercises real
QUIC/TLS, there is no multi-tenancy / RBAC / audit, and there are no
fault-injection / partition / Jepsen-style correctness tests. These are not
polish items — any one of them blocks a compliance-bound pilot.

---

## 1. Durability and Data Integrity

### Raft consensus + WAL

- WAL is real: `WendelonLogStore::open` (`src/consensus/raft_storage.rs:279`)
  does `replay_wal`, handles checkpoint records (`record_type=3`), reopens
  `O_DIRECT` fds, and returns a `DurableBootstrapMode::Recovered`
  discriminant consumed by `node.rs:577`.
- Log append path submits page-aligned writes via io_uring, awaits every
  write CQE, then issues an fsync and awaits it
  (`raft_storage.rs:615–669`). The ordering note on io_uring fsync is
  correct and respected. Vote save is also durable
  (`raft_storage.rs:516–558`).
- Recovery tests exist:
  `recovery_tests::normal_restart_recovers_vote_wal_and_state_machine`,
  `checkpoint_restart_clamps_e_safe_and_replays_only_committed_prefix`,
  plus a torn-tail truncation test.
- Slab per-record checksum is blake3-prefix (`storage/core.rs:554`); plan
  drift vs CRC32c is now documented in
  `plans/adr/010_serialization_tiers.md` and acknowledged.

### fsync-before-quorum

- Replica side (`data_server.rs::handle_replicate`, 2083–2148) calls
  `SlabManager::append_durable` which fsyncs before returning
  `ReplicaAckLevel::DURABLE`.
- Leader side (`data_server.rs:2320–2380`) performs a local
  `append_durable` before fan-out, then counts `Some(Ok(Ok(level)))` if
  `level == ReplicaAckLevel::DURABLE` — non-durable acks are deliberately
  ignored. If fewer than `(RF/2)+1` durable acks arrive, the write returns
  `WriteStatus::INTERNAL_ERROR` rather than finalizing.
- A chaos test
  `write_counts_only_durable_quorum_acks_across_replica_crash_boundary`
  (`data_server.rs:4923`) asserts the invariant.

### What's still missing or weak (P0 / P1)

- **No background scrubber / bit-rot detection.** Record checksums are
  validated on read (`storage/core.rs:517`), so a corrupted page is
  detected when next read. There is no proactive sweeper over cold L1
  slabs and no verification of L2 objects against a stored manifest hash.
  For archival data that may sit for years, silent corruption can
  accumulate undetected.
- **No split-brain / partition tests.** The gap-analysis §5 noted this
  and it is still true — `tests/*.rs` contains no fault injection beyond
  the single fsync-crash test. openraft provides the theoretical
  guarantees, but there is no Jepsen/MadSim harness asserting
  linearizability under partition.
- **HLC drift eviction** is now Raft-proposed (`node.rs:616`
  `ControlRingMembershipAdvancer`), but `spawn_drift_monitor` still calls
  `std::process::abort` on self-drift (`consensus/monitor.rs`). That is
  correct for a frozen node but means a single bad NTP hiccup kills the
  leader hard — there is no soft-fence / step-down-first option.
- **Snapshot install path is single-chunk `Cursor<Vec<u8>>`.** A 100 GB
  state-machine snapshot will buffer in RAM before being applied. No
  compression, no chunked streaming.
- **Group commit on the WAL is absent:** every `append_to_log` batch
  issues its own fsync; there is no coalescer. Under concurrent Raft
  entries this is a latency tax, not a correctness bug.

---

## 2. Object Storage Tiering (S3/GCS)

### What's real

- `RemoteObjectStore` (`src/storage/l2.rs:197`) is a proper
  `opendal::services::S3` wrapper with retry layer, jitter, bounded
  delay, `allow_anonymous`, and virtual-host-style toggles. Supports S3,
  RustFS, MinIO, GCS S3-compat, Backblaze B2 per the docstring.
- Path traversal / bucket substitution is checked on every
  `key_from_uri` call (`l2.rs:257–303`).
- L1→L2 tiering via `SlabSweeper` with two-phase GC (ADR 006 "Done" in
  the gap doc, still present).
- L2→L1 rehydration policy is implemented
  (`storage/manager.rs::RehydrationPolicy`), with thrash-guard metrics
  exposed.
- Migration is transactionally safe: `MigratedRangeTombstone` +
  `SlabManager::append_migration` idempotency + `reclaim_tombstoned_ranges`
  (`manager.rs`).
- Mid-upload crash: `LocalFsObjectStore::put` uses `.tmp` + rename;
  `RemoteObjectStore` relies on opendal retries. The sweeper's two-phase
  GC (L2 pointer durably applied via Raft before L1 unlink) means an S3
  upload that succeeded but whose Raft commit failed simply orphans an
  object — data is not lost.

### Gaps for a Fortune 500 deployment

- **Orphan GC for L2 is not implemented.** Partially-uploaded or
  pointer-committed-but-never-unlinked objects will accumulate. There is
  no list + reconcile loop comparing the `tiered_slabs` Raft state
  against the bucket contents. For a petabyte-scale deployment this is a
  real cost bleed.
- **No range-GET / multipart uploads.** The `ObjectStore` trait is a
  three-verb shape; `get_bytes` pulls the whole slab (typical slab is
  64 MiB, configurable up to GBs). A point read that misses L1 pulls the
  entire slab from S3 every time. The plan acknowledges this but has not
  resolved it.
- **No L2 object encryption.** No SSE-KMS, no client-side envelope
  encryption, no integration with a KMS. For a bank or healthcare
  tenant, this alone fails compliance.
- **No per-bucket / per-tenant L2 config.** The config is a single
  `ObjectStoreConfig` per node (`node.rs:129`); you cannot route tenant
  A to one bucket and tenant B to another.
- **`config.rs:231` hardcodes `l2: None`.** The TOML loader silently
  drops any L2 config. The binary cannot be configured to use S3 without
  code changes. This is a real and easy-to-miss production gap.
- **No region/endpoint failover**, no cross-region replication checks,
  no S3 Object Lock integration for WORM/retention policies.
- **No background scrub of L2.** Same bit-rot issue as L1 but with the
  additional twist that the blake3-per-record validation only fires when
  the slab is actually read. An S3 object silently corrupted during
  transit will not be detected until first read.

---

## 3. High Availability and Operations

### Implemented

- Control Ring RPC surface (`control_server.rs:1076`): `STATUS_REQ`,
  `LEASE_REQ`, `TRANSFER_REQ`, `EPOCH_ADV_REQ`, `JOIN_REQ`. Epoch
  reconciler task runs every 50 ms (`control_server.rs:167`).
- Multi-node join via seed list (`node.rs::request_join_via_seeds`,
  819–905).
- Graceful drain: `ServerLifecycle::begin_drain` +
  `wait_for_quiescence`; `main.rs:87` wires `ctrl_c` to it.
- Rolling upgrade: `ProtocolVersionWindow` + `negotiate_protocol` on
  JOIN.
- Prometheus exporter: 28 counters/gauges, `/metrics` endpoint
  (`metrics/exporter.rs`). Metrics exposed from write/read/query,
  leases, migration, tiering, drift.
- Backup/restore: `ControlRingBackupImage` + `StorageBackupImage` with
  manifest, size, content hash, idempotent restore into a fresh dir.
- Migration auto-seed on epoch transition is implemented via
  `build_seed_batch` / `select_bootstrap_epoch`
  (`control_server.rs:1116–1203`).

### Critical gaps

- **`main.rs` cannot serve QUIC.** There is no `attach_quic`, no
  `serve_quic`, no `quinn::Endpoint::server` call in the binary.
  `WendelonNode::serve_quic` exists (`node.rs:675`) but is not called
  from main. The production daemon literally does not listen on the
  network.
- **`main.rs` cannot join an existing cluster.**
  `WendelonNode::bootstrap` (`node.rs:379`) explicitly errors with
  `ErrorKind::Unsupported` if `cluster.mode = "join"`, saying
  `"cluster.join requires a real outbound transport; use
  bootstrap_with_provider until transport endpoint wiring is attached."`
  So despite `tests/multinode_bootstrap.rs`, the binary only supports
  single-node bootstrap.
- **`config.rs` has no TLS material fields.** No `cert_path`, `key_path`,
  `ca_path`. `TlsMaterial::load` exists but is not reachable from a
  TOML-configured binary. Consequence: the entire mTLS posture in
  `tls.rs` is only exercised via tests that manually construct
  `TlsMaterial`.
- **No distributed tracing.** `tracing` is initialized but there is no
  OTLP / Jaeger / Tempo exporter, no `tracing-opentelemetry`. An SRE
  team cannot follow a write across nodes.
- **No structured audit log.** Lease grants, membership changes,
  backups, and admin RPCs all pass through Raft but there is no
  append-only audit stream a SIEM can consume. Fortune 500 compliance
  (SOX, HIPAA, SOC2) requires this.
- **No operator CLI.** Backup and restore exist as library calls; there
  is no `wendelon backup`, `wendelon restore`, `wendelon cluster-status`,
  `wendelon transfer-leader` CLI surface. An SRE has to write Rust to
  perform any of these.
- **No dynamic reconfiguration beyond membership.** Changing RF,
  rebalancing shards, changing slab size, or retiring a data dir
  requires a restart. No rebalancer at all — `replicas_for` does not
  enforce rack/AZ diversity.
- **No systemd / container integration:** no readiness probe, no health
  endpoint, no SIGTERM → graceful drain wired to k8s preStop semantics
  (ctrl_c is caught but no separate probe port).
- **Single metrics port, single format.** No OpenMetrics, no
  push-gateway, no remote-write. The exporter is a bare TCP listener,
  not a hardened HTTP server — a malformed request path returns 404 but
  there is no request-size cap, no timeout, no auth.

---

## 4. Performance and Concurrency

### Strong choices

- io_uring thread model is clean: one thread per ring, owns `in_flight`
  map keyed on monotonic cookie, fires oneshot replies
  (`storage/io_uring.rs`). Correctly uses `O_DIRECT` with `AlignedBuf`.
- Blake3 per-record checksum is both fast and robust.
- QUIC + zero-copy frame path (`network/core.rs::HrftFrame`) with a
  `BufPool` to amortize allocations.
- `ReplicaRouter` fan-out uses `JoinSet` and stops polling once quorum
  is reached (`data_server.rs:2366`), allowing slower peers to keep
  running without blocking the client.

### Concerns

- **DashMap usage is pervasive and unbounded.** The idempotency cache
  (`data_server.rs::idempotency`), the lease cache, the slab
  `key_index`, and the migration registry are all unbounded DashMaps.
  The gap doc flagged that the idempotency cache "has no TTL sweep
  task"; that is still true. Under sustained client write load with
  random UUIDs, memory grows without bound.
- **No bounded admission control.** There is no per-connection or
  per-peer concurrency cap, no per-stream queue depth cap, no total
  in-flight write cap. A single misbehaving client can spawn unbounded
  QUIC bidi streams (one `tokio::spawn` per stream in
  `network/server.rs:423`). Every spawn allocates a `ReplicaWriteMeta`,
  a frame buffer, and parks on quorum — OOM is one pathological client
  away.
- **Unbounded Arrow buffers on read path.**
  `execute_query_with_migration_merge` concatenates batches into a
  single response (`data_server.rs:1042`). The gap doc flagged "a query
  that returns > working-set RAM will OOM" and that is still true even
  after streaming query responses landed — the merge path still
  collects into one batch.
- **No memory pressure pushback to clients.** No equivalent of BUSY, no
  adaptive concurrency limit, no `ReadStatus::OVERLOADED`.
- **Lock contention is manageable but unmeasured.** The leader-write
  path takes the `SlabManager::state` mutex inside `append_on_target`,
  then releases before fsync. Under high RF=3 fan-out with a hot
  partition this will serialize on one mutex per slab. There are no
  benchmarks asserting the "100 ns-scale hot path" claim from
  `plans/ops/benchmarks.md` — the gap doc called this out; still
  unverified.
- **No NUMA pinning, no CPU affinity** for the io_uring thread or the
  tokio workers. For a 2-socket box under heavy load this matters.
- **`panic = "abort"` in release (`Cargo.toml`).** Good for correctness
  after detection, but combined with `std::process::abort` on drift and
  an unrecoverable disk error panic in the IO thread, any single
  unexpected condition takes the node down hard. There is no
  `catch_unwind` or soft-fence path.

---

## 5. Security and Compliance

### Implemented

- mTLS with `WebPkiClientVerifier` requiring both sides to present a
  cert chaining to the cluster CA. ALPN pin (`wendelon-hrft`) on both
  sides. (`network/tls.rs:123–153`.)
- SAN → NodeId binding via custom DER parser (`tls.rs:313–373`),
  enforced at accept (`server.rs:381`). Hard-rejects missing SAN,
  multiple canonical SANs, non-numeric node ids.
- Ed25519 lease signing (`network/lease_signing.rs`) and TransferToken
  signing (`network/transfer_signing.rs`), verified on apply.
  `LEASE_SIGNED_LEN` correctly excludes the signature field from the
  signed payload.
- Trusted-signer map keyed by node id, loaded from TOML
  (`config.rs::trusted_lease_signers`).

### Severe gaps

- **No encryption at rest.** Grep across `src/` for encryption
  primitives returns only `tls.rs`. Slab files on NVMe and objects in
  S3 are written in the clear. Arrow IPC payloads, WAL pages,
  snapshots — all plaintext on disk. This alone disqualifies the system
  for any regulated industry (finance, health, government). Adding it
  is load-bearing work: envelope encryption, KMS integration, key
  rotation, per-tenant keys, crypto-erase support.
- **Dev key material is deterministic.**
  `dev_lease_signing_secret(node_id) = blake3("wendelon-dev-lease-signing-key-{id}")`
  (`lease_signing.rs:80`). The starter `wendelon.toml` template
  substitutes these in (`config.rs:48–57`). An operator who runs
  `--write-example-config` and edits only the `node_id` gets a cluster
  whose signing keys are guessable by anyone who has read this
  codebase. There is no warning in `render_example_config`.
- **No authorization on Control Ring RPCs.** `advance_epoch`,
  `transfer_leadership`, and `acquire_lease` are reachable by anyone
  who has a valid mTLS cert — no role claim, no scope check. A
  compromised data node can force a leadership transfer or an epoch
  advance. The mTLS chain authenticates peerhood, but the Control Ring
  does not distinguish "operator" from "data node".
- **No hot cert rotation.** `TlsMaterial::load` is one-shot
  (`tls.rs:36` notes this explicitly). A 90-day cert rotation requires
  a full cluster restart.
- **No key rotation for Ed25519 lease/transfer keys.** The trusted map
  is loaded at boot and frozen. Key compromise requires a config edit +
  restart on every node. No key-id field in `LeaseDescriptor` to
  support overlap during rotation.
- **No FIPS mode.** rustls is configured with `ring`, not `aws-lc-rs` +
  FIPS. For federal/financial procurement this is a checklist failure.
- **No audit log.** Admin actions (lease grant, membership change,
  epoch advance, leadership transfer, backup export) are committed
  through Raft but there is no consumer that serializes them into a
  tamper-evident audit stream.
- **TLS SAN binding is cosmetic in dev mode.** The binding enforces
  `node-<id>.cluster.local`, but the canonical domain is hardcoded
  (`DEFAULT_CLUSTER_DOMAIN = "cluster.local"`). No per-cluster domain,
  no CA constraint that limits a CA to issuing only its own cluster's
  node ids.
- **No intrusion detection / rate limiting.** A malformed HRFT frame
  returns `InvalidData`; a peer that floods malformed frames just gets
  their connection closed — there is no ban list, no per-source-IP
  rate limit, no failed-handshake counter feeding a blocklist.
- **No secret redaction.** tracing spans log peer addresses and
  sometimes payload lengths. There is no structured redaction policy
  for sensitive payloads.

---

## 6. Verdict and Roadmap

### Overall Readiness Score: 42–48%

Broken down:
- Core consensus, storage, write path: **85%** — genuinely solid.
- Client SDK & operational RPCs: **70%** — usable, rough edges.
- Production binary + config: **40%** — `main.rs` can't bind QUIC or
  join.
- Security & compliance: **25%** — no encryption at rest, no RBAC, no
  audit.
- Observability & tooling: **35%** — metrics yes, tracing/CLI no.
- Test posture for a storage layer: **30%** — no real-transport, no
  fault-injection, no partition tests.

The architectural bones are good, and the gap-closure push shows
disciplined execution. But "passes the test suite" is a very low bar
for a system that will hold mission-critical data, and several of the
gaps below would be caught immediately in a Fortune 500 security
review.

### Critical Blockers (P0) — must resolve before pilot

1. **Encryption at rest.** Per-tenant envelope encryption with a KMS
   hook (AWS KMS, Google Cloud KMS, HashiCorp Vault Transit). Extends
   to WAL pages, slab records, snapshots, and L2 objects (SSE-KMS or
   client-side). This is a design-first item, probably 4–8 weeks of
   serious work.
2. **Wire `main.rs` to a production bootstrap.** `config.rs` must load
   TLS material; `main.rs` must call `attach_quic` / `serve_quic`;
   `bootstrap` must accept `ClusterBootstrapConfig::Join` against a
   real QUIC provider. Today the binary is a dev toy that the test
   suite has out-grown.
3. **Real-network integration tests.** At least one test harness that
   spins 3 nodes, each with its own `quinn::Endpoint` + mTLS, exercises
   JOIN, WRITE, READ, failover, and backup/restore. Without this,
   every claim in §1–§3 above is only true of the in-process path.
4. **Authorization on Control Ring.** Distinguish "data node" from
   "operator" identities in the cert chain (certificate OID extension
   or separate CA). Enforce at the control handlers.
5. **Partition / fault-injection test suite.** `madsim`, `turmoil`, or
   a deterministic network simulator. Minimum coverage: majority
   partition survives, minority does not accept writes, rejoining
   minority catches up, single-node crash during `append_durable`
   between WAL and slab does not lose an acknowledged write, quorum
   write with one crash during replication replays on recovery.
6. **Admission control / backpressure.** Bounded in-flight writes,
   idempotency cache TTL, per-peer connection cap. An OOM under load
   is a durability outage.
7. **Audit log stream.** Even a simple append-only NDJSON file on each
   node for admin actions, tied to `NodeId` + Raft log index. SOX /
   SOC2 auditors will ask.

### P1 — before wider rollout

8. L2 orphan GC + manifest reconciliation.
9. Range-GET / multipart upload for L2.
10. Background scrubber with manifest hashes for both L1 and L2.
11. Key rotation for TLS certs and Ed25519 keys (key-id field + overlap
    window).
12. Operator CLI (`wendelon backup`, `wendelon restore`,
    `wendelon cluster-status`, `wendelon transfer-leader`,
    `wendelon drain`).
13. Distributed tracing via OTLP.
14. Group commit on the WAL.
15. Snapshot chunking + compression.
16. Per-tenant namespaces, quotas, and per-tenant keys. The plans
    document `multi_tenancy.md` but there is no implementation.

### Architectural Risks (design-level rewrites likely)

- **No authorization model.** Retrofitting multi-tenancy and RBAC into
  a system that has none today is not a sprint item — it cascades into
  the schema (namespace prefixes), the ring (per-namespace routing),
  the lease (per-namespace grants), and the audit log. Budget a
  quarter.
- **`panic = "abort"` + `std::process::abort` in drift monitor +
  unrecoverable panics in the IO thread.** A Fortune 500 data platform
  expects tight control over restart behavior; today any of these
  conditions drops the node from quorum with no telemetry about the
  cause beyond a stderr line. Revisit the process-crash discipline —
  either commit to "fail fast" everywhere (and invest in crash-log +
  core-dump tooling) or introduce a soft-fence mode.
- **`ObjectStore` trait is whole-object.** If range-GET becomes
  important (and it will, for cost and latency), the trait needs a
  breaking change. Do it now, not after a year of production.
- **Unbounded in-memory idempotency / lease / registry maps.** Without
  TTLs, leader-side state grows monotonically. This is a latent
  footgun; under a year of production uptime it will surface.
- **Configuration split-brain.** `NodeBootstrapConfig` has ~15 fields
  that the TOML loader populates, but `l2`, `protocol`, and TLS
  material are all dropped on the floor. As operations grow, the gap
  between "what the config supports" and "what the binary actually
  uses" will be a persistent source of incidents.

### Time estimate to pilot-ready

Taking the P0 list as truly required:
- Encryption at rest: **6–10 weeks** (design + KMS integration + key
  rotation).
- Production bootstrap + QUIC wiring + real-network tests: **3–4
  weeks**.
- Partition test harness + fixing what it finds: **4–6 weeks** (the
  "fixing what it finds" is the unbounded part).
- Authorization + audit log: **4–6 weeks**.
- Admission control + backpressure: **2–3 weeks**.

**Realistic calendar:** 4–6 months of focused work by a competent team
before a pilot with a single internal tenant. Another 6–9 months before
the P1 list is closed and the system is externally defensible in a
Fortune 500 security review. The current codebase is a very credible
architectural prototype — it is not a product.

---

## 7. Addendum — Velocity Recalibration

The Section 6 timeline assumed traditional human-team cadence. Clarifying
context from the architect: the entire codebase was started four days
before this review, authored by an orchestrated swarm of Claude, Codex,
and Gemini agents working against a principal architect's vision. That
changes the shape of the estimate substantially, though not uniformly.

### What compresses dramatically with AI-swarm velocity

- Encryption-at-rest (envelope + KMS integration): weeks → days
- `main.rs` QUIC wiring + TOML TLS/L2 loader: days
- Real-network integration test harness (3 nodes, actual
  `quinn::Endpoint`): days
- Control Ring authz + audit log: days to a week
- Admission control + bounded DashMaps + idempotency TTL: days
- Range-GET / multipart / orphan GC: a week or two

At the velocity demonstrated by the 2026-04-15 → 2026-04-17 gap-closure
push (30+ commits closing the prior assessment's P0 list in ~48 hours),
**code-complete P0** is plausibly 2–4 weeks of orchestrated work, not
months. The P1 list falls in a similar window.

### What does not compress — and why it tightens the Fortune 500 estimate

1. **Bug-discovery-in-anger.** `madsim` / `turmoil` can execute years
   of logical time in hours, so partition and linearizability bugs
   compress well. The long tail — NIC firmware quirks, NVMe
   power-loss modes, S3 eventual-consistency edge cases, `io_uring`
   kernel regressions — only surfaces under real hardware × real
   wall-clock time. No codegen velocity shortens this.
2. **SOC2 Type II observation window.** Six months minimum, not
   negotiable. This is a calendar, not code.
3. **FIPS 140-3 validation** (if the target tenant requires it). Lab
   queues are measured in months independent of codebase readiness.
4. **Procurement + security review + reference customers.** Fortune
   500 buyers do not sign mission-critical storage contracts against
   a system that is weeks old, regardless of code quality. Zero
   precedent means an expensive first deal and a slow one.

### Revised timeline for the stated target

- Code-complete P0 + P1: **3–6 weeks** (bounded by swarm velocity).
- Pilot-ready for an internal tenant: **2–3 months** (bounded by
  real-hardware burn-in and several iterations of madsim-surfaced
  fixes).
- Fortune 500 mission-critical defensible: **9–14 months** (bounded
  by SOC2 Type II observation window + FIPS certification + first
  reference customer — not by code).

### Caveat worth naming

The review found real gaps (`main.rs` does not bind QUIC, `l2: None`
hardcoded in the config loader, deterministic dev signing keys, no
encryption at rest, no real-network integration tests) that came from
high-velocity code generation without a production-burn feedback loop.
As the swarm scales, the binding constraint shifts from "write the
code" to "notice when the code does not actually run end-to-end." That
feedback loop — a daily or per-PR real-network smoke test against a
real `quinn::Endpoint` and a real S3 endpoint — is worth instrumenting
explicitly. Without it, the swarm's surface-area advantage is partially
offset by silent wiring gaps that only a human operator trying to
deploy the thing would catch.
