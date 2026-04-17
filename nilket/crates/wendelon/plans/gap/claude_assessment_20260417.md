# Wendelon Consolidated Gap Assessment — Fortune 500 Readiness

**Date:** 2026-04-17
**Lead reviewer:** Claude (Principal Database Architect)
**Peer inputs incorporated:** Codex evaluation (2026-04-17), Gemini evaluation (2026-04-17)

---

## Executive Summary

Wendelon has undergone an intense ~48-hour push (30+ commits between the
2026-04-15 gap assessment and today) that closes most of the architectural
gaps the prior `plans/gap/*` analyses flagged. The internals are substantive:
~25k LOC of Rust, ~7.2k LOC of tests, and the correctness-shaped gaps
(fsync-before-quorum, WAL recovery, TLS SAN binding, Control Ring RPCs,
lease Ed25519, migration automation) are now genuinely implemented — not
stubbed.

However, this consolidated assessment — combining independent reviews from
three evaluators — reveals that the prior assessments **understated** several
categories. The three reviews converge on the same core verdict but differ
in severity. The gap between "library code quality" and "shippable product"
is wider than any single review captured:

1. **Encryption at rest is entirely absent** (unanimous across all three reviews).
2. **The production binary (`main.rs`) cannot bring up a multi-node TLS cluster** (unanimous).
3. **No real-network integration tests exercise QUIC/TLS** (unanimous).
4. **Snapshot persistence and WAL compaction are unfinished** — snapshots are only held in memory; no live path writes `snapshot.wnd` to disk; WAL `truncate()` and `purge()` mutate only the in-memory cache (Codex finding, verified).
5. **Follower disk leak** — `TierObserver` is not wired into `node.rs` bootstrap, so followers never unlink L1 slabs after tiering (Gemini finding, verified).
6. **JOIN_REQ identity binding bug** — the join handler trusts `req.node_id` from the wire payload instead of binding it to the authenticated transport peer identity, enabling membership poisoning (Codex finding, verified).
7. **No multi-tenancy / RBAC / audit** (unanimous).
8. **No fault-injection / partition / Jepsen-style correctness tests** (Claude + Codex).
9. **Unbounded memory growth** in idempotency cache, lease cache, key index, and migration registry (Claude + Codex + Gemini).

### Readiness Score Reconciliation

| Reviewer | Score | Rationale |
|----------|-------|-----------|
| Claude   | 42–48% | Weighted toward architectural soundness of internals |
| Codex    | 28%    | Weighted toward "does the shipped binary actually work end-to-end" |
| Gemini   | 65%    | Weighted toward happy-path code quality |

**Consolidated score: 35–40%.** Codex's emphasis on the daemon not being a
real cluster process and the unfinished snapshot/compaction lifecycle pulls
the score down from my original estimate. Gemini's 65% overstates readiness
by underweighting the gap between library code and production wiring. The
core consensus and storage write path are genuinely solid (~85%), but
multiple load-bearing subsystems exist only as library code that the
production binary does not exercise.

---

## 1. Durability and Data Integrity

### Raft Consensus + WAL

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

- **Snapshot persistence is memory-only (P0).** `install_snapshot()`
  (`raft_storage.rs:1008–1025`) and `build_snapshot()`
  (`raft_storage.rs:1309–1335`) both store bytes only in
  `guard.snapshot`. Neither writes `snapshot.wnd` to disk. The recovery
  path *can* load `snapshot.wnd` (`raft_storage.rs:1405–1460`), but no
  live path creates it. This means the snapshot lifecycle exists on paper
  and in tests, not as a production steady-state durability mechanism.
  After a crash, the node has no snapshot to recover from — only the raw
  WAL, which grows without bound because compaction is also incomplete.
  *(Codex finding, verified.)*
- **Checkpoint emission does not exist in production.** Recovery
  understands `REC_CHECKPOINT` (`raft_storage.rs:90`, `1589–1632`,
  `1808–1822`), but no production code path writes checkpoint records.
  Without live checkpoints, restart-time clamping of commit / `E_safe`
  depends on tests rather than runtime behavior. *(Codex finding,
  verified.)*
- **WAL truncation and purging are not durable.** `truncate()`
  (`raft_storage.rs:677`) and `purge()` (`raft_storage.rs:697`) only
  mutate the in-memory cache and explicitly defer WAL rewrites/compaction.
  WAL growth is therefore unbounded and log compaction is not durably
  realized on disk. *(Codex finding, verified.)*
- **No background scrubber / bit-rot detection.** Record checksums are
  validated on read (`storage/core.rs:517`), so a corrupted page is
  detected when next read. There is no proactive sweeper over cold L1
  slabs and no verification of L2 objects against a stored manifest hash.
  For archival data that may sit for years, silent corruption can
  accumulate undetected. *(Unanimous.)*
- **No split-brain / partition tests.** `tests/*.rs` contains no fault
  injection beyond the single fsync-crash test. openraft provides the
  theoretical guarantees, but there is no Jepsen/MadSim harness asserting
  linearizability under partition. *(Claude + Codex.)*
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
  entries this is a latency tax, not a correctness bug. *(Gemini also
  flagged this as P0 for device-media durability under volatile write
  cache assumptions.)*
- **Startup time scales with data volume.** `SlabManager::open()` scans
  every slab file and `recover_slab()` walks each file record-by-record
  to rebuild indexes (`storage/manager.rs:324–387`,
  `storage/core.rs:467–539`). At enterprise scale this creates poor
  restart SLOs. *(Codex finding.)*
- **Local slab recovery treats a bad checksum as "stop here"**
  (`storage/core.rs:517–523`). Appropriate for torn trailing writes, but
  not a full interior-corruption strategy with block quarantine, explicit
  bad-sector reporting, or automatic replica repair. *(Codex finding.)*

---

## 2. Object Storage Tiering (S3/GCS)

### What's real

- `RemoteObjectStore` (`src/storage/l2.rs:197`) is a proper
  `opendal::services::S3` wrapper with retry layer, jitter, bounded
  delay, `allow_anonymous`, and virtual-host-style toggles. Supports S3,
  RustFS, MinIO, GCS S3-compat, Backblaze B2 per the docstring.
- Path traversal / bucket substitution is checked on every
  `key_from_uri` call (`l2.rs:257–303`).
- L1->L2 tiering via `SlabSweeper` with two-phase GC (ADR 006).
- The sweeper verifies object integrity by round-tripping the uploaded
  blob and comparing hashes before committing the pivot
  (`sweeper.rs:263–323`).
- L2->L1 rehydration policy is implemented
  (`storage/manager.rs::RehydrationPolicy`), with thrash-guard metrics
  exposed. Rehydration writes to a temp file, fsyncs, and renames before
  opening the local copy (`storage/manager.rs:1252–1307`).
- Migration is transactionally safe: `MigratedRangeTombstone` +
  `SlabManager::append_migration` idempotency + `reclaim_tombstoned_ranges`
  (`manager.rs`).

### Gaps for a Fortune 500 deployment

- **CRITICAL: Follower disk leak (P0).** The `WendelonStateMachine` in
  `src/node.rs` is NOT wired with a `TierObserver` — `with_tier_observer`
  is never called during bootstrap (`node.rs:427–441`). While the leader's
  sweeper unlinks L1 files after tiering, followers apply the Raft
  `TierSlab` entry but never trigger the local `unlink`. This results in
  followers' NVMe disks filling up indefinitely with "tiered" slabs that
  are never deleted locally. *(Gemini finding, verified — `with_tier_observer`
  is absent from node.rs bootstrap.)*
- **Orphan GC for L2 is not implemented.** If the process crashes after
  `put()` succeeds but before the Raft commit lands, the S3 object is
  orphaned. There is no pending-upload journal, no list + reconcile loop
  comparing `tiered_slabs` Raft state against bucket contents, and no
  orphan-GC mechanism. For a petabyte-scale deployment this is a real
  cost bleed. *(Claude + Codex.)*
- **Followers can pay the full upload cost and still fail the commit.**
  `tier_one_slab()` uploads before calling the Raft committer
  (`sweeper.rs:272–310`). On a follower, that means wasted uploads and
  potential object leaks. *(Codex finding.)*
- **Tiered-slab reconciliation is incomplete on followers and restart.**
  `apply_tier_slab()` inserts into `state.tiered` before unlinking the
  local file (`storage/manager.rs:634–655`). If unlink fails, the state
  mutation is already done and retries short-circuit. In production
  bootstrap, no `ManagerTierObserver` is attached to the recovered state
  machine (`node.rs:437–441`). *(Codex finding, verified.)*
- **Crash safety during local compaction is weak.** The replacement slab
  is created and fsynced, then in-memory maps are updated, then the old
  file is deleted (`storage/manager.rs:1062–1149`). If the process dies
  mid-compaction, restart rescans both files, potentially re-surfacing
  stale data or duplicate versions. *(Codex finding.)*
- **No range-GET / multipart uploads.** The `ObjectStore` trait is a
  three-verb shape; `get_bytes` pulls the whole slab (typical slab is
  64 MiB, configurable up to GBs). A point read that misses L1 pulls the
  entire slab from S3 every time. *(Unanimous.)*
- **No L2 object encryption.** No SSE-KMS, no client-side envelope
  encryption, no integration with a KMS.
- **No per-bucket / per-tenant L2 config.** The config is a single
  `ObjectStoreConfig` per node (`node.rs:129`).
- **`config.rs:231` hardcodes `l2: None`.** The TOML loader silently
  drops any L2 config. The binary cannot be configured to use S3 without
  code changes.
- **No L2 object deletion lifecycle.** The only normal `delete()` call
  is best-effort cleanup on a round-trip hash mismatch
  (`sweeper.rs:283–288`). No path deletes cold objects after retention
  expiry, remote compaction, or migration cleanup. *(Codex finding.)*
- **No background scrub of L2.** Same bit-rot issue as L1 but with the
  additional twist that blake3 validation only fires when the slab is
  actually read. An S3 object silently corrupted during transit will not
  be detected until first read.

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
- Minority partition eviction guard is a real strength
  (`tests/membership_advancement.rs:214–286`).

### Critical gaps

- **`main.rs` cannot serve QUIC (P0).** There is no `attach_quic`, no
  `serve_quic`, no `quinn::Endpoint::server` call in the binary.
  `WendelonNode::serve_quic` exists (`node.rs:675`) but is not called
  from main. The production daemon literally does not listen on the
  network. *(Unanimous.)*
- **`main.rs` cannot join an existing cluster (P0).**
  `WendelonNode::bootstrap` (`node.rs:379`) explicitly errors with
  `ErrorKind::Unsupported` if `cluster.mode = "join"`. The binary only
  supports single-node bootstrap. *(Claude + Codex.)*
- **`config.rs` has no TLS material fields (P0).** No `cert_path`,
  `key_path`, `ca_path`. `TlsMaterial::load` exists but is not reachable
  from a TOML-configured binary. The entire mTLS posture in `tls.rs` is
  only exercised via tests. *(Claude + Codex.)*
- **`main()` never installs a tracing subscriber.** The many
  `tracing::warn!` / `tracing::info!` calls are not meaningfully surfaced
  by the stock daemon. *(Codex finding.)*
- **No distributed tracing.** No OTLP / Jaeger / Tempo exporter, no
  `tracing-opentelemetry`. An SRE team cannot follow a write across nodes.
  *(Unanimous.)*
- **No structured audit log.** Lease grants, membership changes,
  backups, and admin RPCs all pass through Raft but there is no
  append-only audit stream a SIEM can consume. Fortune 500 compliance
  (SOX, HIPAA, SOC2) requires this. *(Unanimous.)*
- **No operator CLI.** Backup and restore exist as library calls; there
  is no `wendelon backup`, `wendelon restore`, `wendelon cluster-status`,
  `wendelon transfer-leader` CLI surface.
- **No dynamic reconfiguration beyond membership.** Changing RF,
  rebalancing shards, changing slab size, or retiring a data dir
  requires a restart. No rebalancer at all — `replicas_for` does not
  enforce rack/AZ diversity. *(Claude + Gemini.)*
- **No systemd / container integration:** no readiness probe, no health
  endpoint, no SIGTERM -> graceful drain wired to k8s preStop semantics.
- **openraft configuration is almost entirely defaulted.** Only
  `cluster_name` is set before validation (`node.rs:477–490`). Election,
  heartbeat, snapshot, compaction, and storage-retention tuning are all
  at openraft defaults. *(Codex finding.)*
- **Single metrics port, single format.** No OpenMetrics, no
  push-gateway, no remote-write. The exporter is a bare TCP listener —
  no request-size cap, no timeout, no auth. No latency histograms or
  dimensional labels. *(Claude + Codex.)*

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
  The idempotency cache "has no TTL sweep task" — under sustained client
  write load with random UUIDs, memory grows without bound. *(Unanimous.)*
- **No bounded admission control.** There is no per-connection or
  per-peer concurrency cap, no per-stream queue depth cap, no total
  in-flight write cap. A single misbehaving client can spawn unbounded
  QUIC bidi streams (one `tokio::spawn` per stream in
  `network/server.rs:423`). OOM is one pathological client away.
- **Unbounded Arrow buffers on read path.**
  `execute_query_with_migration_merge` concatenates batches into a
  single response (`data_server.rs:1042`). A query returning > working-set
  RAM will OOM.
- **L2 hot-path memory behavior is poor:** tiering uploads the full slab
  into memory and downloads it back for verification; L2 reads download
  the full slab to fetch one record; rehydration keeps a second
  in-memory copy while writing the local file. *(Claude + Codex.)*
- **Write path serializes full Arrow `RecordBatch` into one `Vec<u8>`**
  before local durable append and remote fan-out
  (`data_server.rs:2289–2294`). Under heavy write load this is an
  allocator hotspot. *(Codex finding.)*
- **Lock contention is manageable but unmeasured.** The leader-write
  path takes the `SlabManager::state` mutex inside `append_on_target`,
  then releases before fsync. Under high RF=3 fan-out with a hot
  partition this will serialize on one mutex per slab. No benchmarks
  verify the "100 ns-scale hot path" claim from `plans/ops/benchmarks.md`.
- **No NUMA pinning, no CPU affinity** for the io_uring thread or the
  tokio workers.
- **`panic = "abort"` in release (`Cargo.toml`).** Combined with
  `std::process::abort` on drift and an unrecoverable disk error panic
  in the IO thread, any single unexpected condition takes the node down
  hard. No `catch_unwind` or soft-fence path.
- **Index scalability.** The entire key index lives in RAM (DashMap). At
  enterprise scale with billions of keys, this will hit memory limits. A
  tiered index (LSM-style or persistent B-Tree) will eventually be
  required. *(Gemini finding.)*
- **Startup/recovery cost scales linearly with slab footprint.** All
  indexes are rebuilt by scanning every slab file. Rolling upgrades and
  restart SLOs will degrade with data volume. *(Codex + Gemini.)*

---

## 5. Security and Compliance

### Implemented

- mTLS with `WebPkiClientVerifier` requiring both sides to present a
  cert chaining to the cluster CA. ALPN pin (`wendelon-hrft`) on both
  sides. (`network/tls.rs:123–153`.)
- SAN -> NodeId binding via custom DER parser (`tls.rs:313–373`),
  enforced at accept (`server.rs:381`). Hard-rejects missing SAN,
  multiple canonical SANs, non-numeric node ids.
- Ed25519 lease signing (`network/lease_signing.rs`) and TransferToken
  signing (`network/transfer_signing.rs`), verified on apply.
  `LEASE_SIGNED_LEN` correctly excludes the signature field from the
  signed payload.
- Trusted-signer map keyed by node id, loaded from TOML
  (`config.rs::trusted_lease_signers`).

### Severe gaps

- **No encryption at rest (P0).** Grep across `src/` for encryption
  primitives returns only `tls.rs`. Slab files on NVMe and objects in
  S3 are written in the clear. Arrow IPC payloads, WAL pages,
  snapshots — all plaintext on disk. This alone disqualifies the system
  for any regulated industry (finance, health, government). Adding it
  is load-bearing work: envelope encryption, KMS integration, key
  rotation, per-tenant keys, crypto-erase support. *(Unanimous.)*
- **CRITICAL: JOIN_REQ identity binding bug (P0).** `handle_control_frame()`
  receives the authenticated `remote: NodeId` from the TLS transport
  (`control_server.rs:1075–1089`). `JOIN_REQ` handling ignores that
  authenticated identity and trusts `req.node_id` from the wire payload
  (`control_server.rs:445–470`, `906–937`). `accept_join()` then adds
  that *claimed* node id into Raft membership and the visible epoch
  (`control_server.rs:937–1003`). A node with any CA-trusted certificate
  can therefore request admission for an arbitrary `node_id` / address
  pair. Even if later dial-back fails, this is a serious admission-control
  and membership-poisoning vulnerability. *(Codex finding, verified.)*
- **Dev key material is deterministic.**
  `dev_lease_signing_secret(node_id) = blake3("wendelon-dev-lease-signing-key-{id}")`
  (`lease_signing.rs:80`). An operator who runs
  `--write-example-config` and edits only the `node_id` gets a cluster
  whose signing keys are guessable by anyone who has read this codebase.
  There is no warning in `render_example_config`.
- **No authorization on Control Ring RPCs.** `advance_epoch`,
  `transfer_leadership`, and `acquire_lease` are reachable by anyone
  with a valid mTLS cert — no role claim, no scope check. A compromised
  data node can force a leadership transfer or epoch advance.
- **No hot cert rotation.** `TlsMaterial::load` is one-shot
  (`tls.rs:36`). A 90-day cert rotation requires a full cluster restart.
- **No key rotation for Ed25519 lease/transfer keys.** The trusted map
  is loaded at boot and frozen. No key-id field in `LeaseDescriptor` to
  support overlap during rotation.
- **No FIPS mode.** rustls is configured with `ring`, not `aws-lc-rs` +
  FIPS. For federal/financial procurement this is a checklist failure.
- **No audit log.** Admin actions are committed through Raft but there
  is no consumer serializing them into a tamper-evident audit stream.
  *(Unanimous.)*
- **TLS SAN binding is cosmetic in dev mode.** The canonical domain is
  hardcoded (`DEFAULT_CLUSTER_DOMAIN = "cluster.local"`). No per-cluster
  domain, no CA constraint limiting a CA to its own cluster's node ids.
- **No intrusion detection / rate limiting.** A peer flooding malformed
  frames just gets their connection closed — no ban list, no rate limit,
  no failed-handshake counter.
- **The metrics endpoint is unauthenticated plaintext HTTP**
  (`metrics/exporter.rs:15–73`). *(Codex finding.)*

---

## 6. Verdict and Roadmap

### Overall Readiness Score: 35–40%

Broken down by subsystem:

| Subsystem | Score | Notes |
|-----------|-------|-------|
| Core consensus + WAL write path | 80% | Solid, but snapshot persistence and WAL compaction are memory-only |
| Slab storage (L1) | 75% | Durable appends real, but follower disk leak, no scrubber, startup scales with data |
| Object storage tiering (L2) | 35% | Building blocks exist; not wired into production; crash semantics incomplete |
| Production binary + config | 25% | Cannot serve QUIC, cannot join, TLS not loadable, L2 hardcoded off, no tracing subscriber |
| Security + compliance | 20% | No encryption at rest, JOIN identity bug, no RBAC, no audit, no cert rotation |
| Client SDK + operational RPCs | 65% | Usable, rough edges |
| Observability + tooling | 30% | Counters/gauges yes; tracing/CLI/histograms/auth no |
| Test posture for a storage layer | 25% | No real-transport tests, no fault-injection, no partition tests |

### Critical Blockers (P0) — must resolve before pilot

1. **Wire `main.rs` to a production bootstrap.** `config.rs` must load
   TLS material; `main.rs` must call `serve_quic`; `bootstrap` must
   accept `ClusterBootstrapConfig::Join` against a real QUIC provider.
   Install a tracing subscriber. Today the binary is a dev toy that the
   test suite has outgrown.

2. **Finish Raft durability lifecycle.** Persist `snapshot.wnd` in the
   live `install_snapshot` / `build_snapshot` paths. Emit real
   `REC_CHECKPOINT` records at runtime. Implement durable WAL
   compaction / purge, not just in-memory cache mutation. Without these,
   WAL growth is unbounded and crash recovery degrades over time.

3. **Encryption at rest.** Per-tenant envelope encryption with a KMS
   hook (AWS KMS, Google Cloud KMS, HashiCorp Vault Transit). Extends
   to WAL pages, slab records, snapshots, and L2 objects (SSE-KMS or
   client-side). Design-first item.

4. **Fix JOIN_REQ identity binding.** `accept_join()` must assert
   `req.node_id == remote` (the authenticated transport peer id).
   Without this, any CA-trusted peer can inject arbitrary membership
   entries — a membership-poisoning vulnerability.

5. **Wire `TierObserver` into `node.rs` bootstrap.** Without this,
   every follower leaks disk space monotonically as tiering proceeds.
   A one-line fix with outsized production impact.

6. **Real-network integration tests.** At least one test harness that
   spins 3 nodes, each with its own `quinn::Endpoint` + mTLS, exercises
   JOIN, WRITE, READ, failover, and backup/restore. Without this,
   every claim in sections 1–3 is only true of the in-process path.

7. **Partition / fault-injection test suite.** `madsim`, `turmoil`, or
   a deterministic network simulator. Minimum coverage: majority
   partition survives, minority does not accept writes, rejoining
   minority catches up, single-node crash during `append_durable`
   between WAL and slab does not lose an acknowledged write.

8. **Admission control / backpressure.** Bounded in-flight writes,
   idempotency cache TTL, per-peer connection cap. An OOM under load
   is a durability outage.

9. **Authorization on Control Ring.** Distinguish "data node" from
   "operator" identities in the cert chain (certificate OID extension
   or separate CA). Enforce at the control handlers.

10. **Audit log stream.** Append-only NDJSON file on each node for
    admin actions, tied to `NodeId` + Raft log index. SOX / SOC2
    auditors will ask.

### P1 — before wider rollout

11. L2 orphan GC + manifest reconciliation + L2 object deletion lifecycle.
12. Range-GET / multipart upload for L2.
13. Background scrubber with manifest hashes for both L1 and L2.
14. Key rotation for TLS certs and Ed25519 keys (key-id field + overlap
    window).
15. Operator CLI (`wendelon backup`, `wendelon restore`,
    `wendelon cluster-status`, `wendelon transfer-leader`,
    `wendelon drain`).
16. Distributed tracing via OTLP.
17. Group commit on the WAL.
18. Snapshot chunking + compression.
19. Per-tenant namespaces, quotas, and per-tenant keys.
20. Crash-safe local compaction (journal the old->new slab swap).
21. Persistent or tiered key index for billion-key scale.

### Architectural Risks (design-level rewrites likely)

- **No authorization model.** Retrofitting multi-tenancy and RBAC into
  a system that has none today cascades into the schema (namespace
  prefixes), the ring (per-namespace routing), the lease (per-namespace
  grants), and the audit log. Budget a quarter at human velocity.
- **State split across persistence domains.** Local files, Raft state,
  and L2 objects can diverge during tiering and compaction. The
  follower disk leak and the compaction crash-safety issue are symptoms
  of a missing unifying transactional manifest. *(Codex risk finding.)*
- **`ObjectStore` trait is whole-object.** If range-GET becomes
  important (and it will), the trait needs a breaking change. Do it now.
- **Unbounded in-memory maps.** Idempotency, lease, key index, and
  registry are all unbounded. Under a year of production uptime this
  will surface.
- **`panic = "abort"` + `std::process::abort` discipline.** A Fortune
  500 data platform expects tight control over restart behavior. Either
  commit to "fail fast" everywhere (and invest in crash-log + core-dump
  tooling) or introduce a soft-fence mode.
- **Configuration gap.** `NodeBootstrapConfig` has ~15 fields that the
  TOML loader populates, but `l2`, `protocol`, and TLS material are all
  dropped on the floor. The gap between "what the config supports" and
  "what the binary actually uses" will be a persistent source of
  incidents.
- **Index scalability.** The all-in-RAM DashMap key index will
  eventually hit memory limits at enterprise scale. A tiered index
  (LSM-style or persistent B-Tree) will be required. *(Gemini risk
  finding.)*

---

## 7. Velocity Recalibration

The Section 6 timeline assumed traditional human-team cadence. The entire
codebase was started four days before this review, authored by an
orchestrated swarm of Claude, Codex, and Gemini agents working against a
principal architect's vision.

### What compresses dramatically with AI-swarm velocity

- Encryption-at-rest (envelope + KMS integration): weeks -> days
- `main.rs` QUIC wiring + TOML TLS/L2 loader: days
- Real-network integration test harness: days
- JOIN identity fix + Control Ring authz + audit log: days to a week
- Admission control + bounded DashMaps + idempotency TTL: days
- TierObserver wiring: hours (one-line fix)
- Snapshot persistence + checkpoint emission + durable WAL purge: days
- Range-GET / multipart / orphan GC: a week or two

At the velocity demonstrated by the 2026-04-15 -> 2026-04-17 gap-closure
push, **code-complete P0** is plausibly 2–4 weeks of orchestrated work.

### What does not compress

1. **Bug-discovery-in-anger.** NIC firmware quirks, NVMe power-loss
   modes, S3 eventual-consistency edge cases, `io_uring` kernel
   regressions — only surface under real hardware x real wall-clock time.
2. **SOC2 Type II observation window.** Six months minimum, not
   negotiable.
3. **FIPS 140-3 validation** (if required). Lab queues are measured in
   months.
4. **Procurement + security review + reference customers.** Fortune 500
   buyers do not sign mission-critical storage contracts against a system
   that is weeks old.

### Revised timeline

- Code-complete P0 + P1: **3–6 weeks** (bounded by swarm velocity).
- Pilot-ready for an internal tenant: **2–3 months** (bounded by
  real-hardware burn-in and madsim-surfaced fixes).
- Fortune 500 mission-critical defensible: **9–14 months** (bounded by
  SOC2 Type II + FIPS + first reference customer — not by code).

### Feedback loop caveat

All three reviewers independently found that the gap between "library
code works in tests" and "the production binary actually runs end-to-end"
is the dominant risk at this velocity. The swarm produces high-quality
component code but does not catch wiring gaps — `main.rs` not binding
QUIC, `config.rs` hardcoding `l2: None`, `TierObserver` not wired,
snapshots not persisted to disk. **A daily or per-PR real-network smoke
test** (actual `quinn::Endpoint`, actual S3 endpoint, multi-node join +
write + read + failover) is the single highest-leverage investment for
this project. It converts the swarm's surface-area advantage from a
liability (silent wiring gaps) into a strength (rapid iteration against
a real integration gate).
