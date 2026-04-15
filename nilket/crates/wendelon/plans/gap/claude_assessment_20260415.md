# Wendelon Gap Assessment — 2026-04-15

Author: Claude (Opus 4.6)
Scope: divergence between `plans/` and the current tree under `src/`.
Method: read-only pass over every file in `plans/` (49 documents including ADRs 001–013,
target plans 012–019, and the `consensus/`, `network/`, `ops/`, `schema/`, `storage/`
subfolders) cross-referenced against ~19.6 kLoC of Rust under `src/`.

This document is a punch list, not a design change. Where a plan item is already
implemented it is listed under **Done** so the remaining work is unambiguous.

**Revision 2026-04-15b:** folded in three structural gaps surfaced by Codex's
parallel assessment (`plans/gap/codex_20260415.md`) — Control Ring server-side
RPC surface, fsync-before-quorum durability semantics, and TLS SAN→NodeId
binding. The per-target structure below is otherwise unchanged.

---

## 1. Per-Target gaps

### Target 012 — Bifurcated write path (HashRaft / Stellarator)

**Done**
- Small-write path: `DataRing::write` validates epoch → idempotency cache →
  lease gate → HighwayHash key → `RingRouter::replicas` → concurrent fan-out via
  `JoinSet` → `(RF/2)+1` quorum → `raft.client_write(WendelonRequest::Finalize(..))`.
- 88-byte `FinalizationMarker` is what enters the Raft log; Arrow IPC bulk
  bypasses it (ADR 003 honored).

**Gaps**
- `WendelonDataServer::stream_finalization` returns an empty `mpsc::channel(1)`
  — the streaming finalization path from `plans/network/write_flow.md` is a
  stub.
- `WendelonDataServer::bulk_write` is likewise a `mpsc::channel(1)` stub; no
  Arrow IPC ingest path for the large-write case.
- `handle_data_frame` in `data_server.rs` only routes `REPLICATE_REQ`,
  `QUERY_REQ`, `MIGRATE_REQ`. Client-facing `WRITE_REQ`, `READ_REQ`, and
  `BULK_WRITE_REQ` are defined in `schema/` but have no server-side
  decoder path — the `DataRing::write` / `read` methods are reachable in
  tests but not from the HRFT wire. (Credit: Codex flagged this framing.)
- Leader-side HLC merge is not enforced. The plan specifies
  `hlc_commit = max(leader_hlc, client_hlc) + 1`; today the client HLC flows
  through `ReadRequestMeta` / `FinalizationMarker` unchanged.
- Idempotency cache exists but the plan-mandated rate-limit / TTL sweep task is
  not observable in `network/idempotency.rs`.

### Target 013 — Client SDK & topology

**Done**
- `WendelonClient` in `client/core.rs` with `refresh_topology`,
  `apply_snapshot`, `query`, and `pick_target` (leader preference).
- One-shot retry on `NOT_LEADER` and `E_SAFE_BEHIND` redirects.
- `StaticTopologySource` implementation of `TopologySource` for tests.

**Gaps**
- No push-based Control-Ring topology subscription task — topology is pull-only.
  `plans/network/api_sdk.md` calls for a long-lived subscribe stream.
- `bulk_write` client entry point missing.
- DataFusion `TableProvider` and PGWire adapters explicitly deferred; not yet
  scaffolded.
- Retry policy is one-shot only; plan specifies exponential backoff with
  jitter and a circuit breaker per peer.
- No streaming result consumer on the client side (pairs with 015 gap below).

### Target 014 — Time-travel reads

**Done**
- `QueryRequestMeta.as_of_hlc` plumbed end-to-end.
- `SlabManager::lookup_as_of` / `scan_as_of` / `scan_all_versions` implemented
  and exercised by `DataRing::read`.
- Migration-aware fallback via `try_migrate_point_read`
  (Hit / AuthoritativeMiss / Incomplete).

**Gaps**
- None structurally. Recommend a soak test that asserts a read at
  `as_of = E_safe - 1` during a concurrent flush + sweep. Not a code gap, but
  the plan's acceptance criterion in `plans/014_time_travel.md` is not yet
  covered by a test.

### Target 015 — Server-side WPBC

**Done**
- `schema/wpbc.rs` interpreter with arithmetic, comparison, projection,
  filter, aggregate opcodes.
- `execute_query_plan` scans via `scan_as_of`, runs `wpbc::execute` per batch,
  concatenates.

**Gaps**
- Results are concatenated into a single response frame. `plans/network/read_flow.md`
  specifies chunked multi-frame streaming keyed on `max_batch_bytes`. The
  client has no streaming consumer to match (see 013).
- No server-side spill threshold; a query that returns > working-set RAM
  will OOM.
- No WPBC program cache by hash — each request re-validates the bytecode.

### Target 016 — Unified hash ring

**Done**
- Virtual-node ring with HighwayHash positions in `network/ring.rs`.
- Capability filtering on the `Data` role; replicas walk clockwise.
- Watch-driven `spawn_updater` rebuilds on membership change.

**Gaps**
- `VNODES_PER_NODE = 64` in `network/ring.rs` vs. `256` in
  `plans/network/topology.md` — config drift. Either reconcile the plan or
  raise the constant.
- Capability filtering for `Compute` and `Coordinator` roles is untested;
  only the `Data` path is exercised.
- `replicas_for` does not enforce rack/AZ diversity — the plan hints at
  anti-affinity but no code implements it.

### Target 017 — Autonomous eviction & HLC drift

**Done**
- `NodeTracker` with `check_drift_once` / `sweep_evictions_once` in
  `consensus/monitor.rs`.
- Constants match the plan: `MAX_OFFSET_MS=500`, `EVICTION_THRESHOLD=300s`,
  `DRIFT_CHECK_INTERVAL=1s`, `EVICTION_CHECK_INTERVAL=30s`.
- `ProcessAbort` hits `std::process::abort` — no graceful-shutdown bypass.

**Gaps**
- `node.rs::WatchMembershipAdvancer` is a local stub that mutates a `watch`
  channel directly. In a real multi-node cluster an eviction decision must be
  proposed as a `ChangeMembership` through Raft; today nothing is committed.
- No Prometheus counter for `hlc_drift_abort_total` (called out in
  `plans/ops/observability.md`).

### Target 018 — Per-range leases

**Done**
- `LeaseDescriptor` in `schema/core.rs` with range/epoch/HLC expiry.
- `raft_storage.rs::apply_request` handles `AcquireLease` with ownership
  precedence.
- Lease-gate on `DataRing::write` and `DataRing::read`.

**Gaps**
- `LeaseDescriptor.signature` is zero-filled in
  `refresh_leases_once` — Ed25519 signing not implemented. The plan ties
  lease validity to a leader signature a peer can verify offline.
- `TransferToken` Ed25519 verification is absent on the receive side.
- Write-proxy path intentionally out of scope per plan — not flagged as a gap,
  just noted.
- Lease refresh jitter / backoff is fixed; plan specifies per-range jitter.

### Target 019 — Lazy migration (Migration Ledger)

**Done**
- `MigrationLedgerEntry`, `SeedMigration`, `MigrationComplete`,
  `MigratedRangeTombstone` all in `consensus/core.rs`.
- `apply_seed_migration` is idempotent on `migration_id`.
- `apply_migration_complete` validates redundant fields, installs a tombstone,
  reconciles descendants (direct upstream link → replace source; mere overlap
  → prepend new physical holder).
- `SlabManager::append_migration` with `MigrationIngestOutcome::{Inserted, Duplicate}`.
- `DataRing::read` fallback through `try_migrate_point_read` is wired.

**Gaps**
- No epoch-transition hook that auto-emits a batch of `SeedMigration` entries
  for every reassigned range when `ring(E)` → `ring(E+1)` is committed.
  Without it, ledger entries must be seeded by an external call that does not
  exist in the tree.
- No background `MigrationSweeper` task driving `MIGRATE_REQ` at the logical
  owner and auto-emitting `MigrationComplete` after drain.
- Tombstone-aware slab rewrite / compaction (Plan 019 step 10) is not
  implemented. Tombstones accumulate; physical reclaim never happens.
- `MIGRATION_INCOMPLETE` error propagates to the client, but the client does
  not yet retry on it with backoff.

---

## 2. Cross-cutting gaps

### Control Ring RPC surface (`plans/network/rpc_traits.md`, `plans/network/api_sdk.md`)

- `src/network/traits.rs` defines a `ControlRing` trait with
  `transfer_leadership`, `acquire_lease`, `advance_epoch`, `get_cluster_status`.
- No server-side `impl ControlRing for …` exists in the tree. The only
  implementor is a client-side stub in `src/client/topology.rs`.
- Consequence: topology/status, lease-acquire, leadership-transfer, and
  epoch-advance RPCs are not reachable from outside the process. The Control
  Ring is exercised today only through in-process `Raft::client_write`.
- Wire-concrete evidence of the gap: `ControlRingTopologySource` in
  `client/topology.rs:221` actually sends `MsgType::STATUS_REQ` and decodes
  `STATUS_RESP`, but `data_server.rs::handle_data_frame` has no arm for
  either message type. The client is already talking; nothing is listening.
  (Sharpened per Codex's revision.)
- This is upstream of several per-target gaps — 013 (topology subscribe), 017
  (Raft-proposed eviction), 018 (`TransferToken` verification), 019
  (epoch-transition auto-seed) all need a real Control Ring endpoint.
- (Credit: Codex flagged this as the single clearest structural gap; I had
  underweighted it by filing pieces into individual targets.)

### Durability boundary — fsync before quorum ack (`plans/network/write_flow.md`)

- The plan's quorum semantics require each replica to return a durable ack
  — the plan language is "quorum of durable acks," and the write path
  advances `E_safe` only after that quorum. The WAL / slab append must be
  `fsync`'d before the replica's `REPLICATE_RESP` counts toward quorum.
- `src/storage/core.rs` and `src/storage/io_uring.rs` have `fsync` /
  `fdatasync` primitives available.
- `src/network/data_server.rs` comments frame quorum as the durability
  boundary ("the slab is the durability boundary for one node; quorum is the
  durability boundary for the write") but the `handle_replicate` path does
  not force an explicit fsync before returning `REPLICATE_RESP` with
  `DurabilityLevel::QUORUM`. A crash between append and page-cache flush
  on `RF/2 + 1` replicas could lose a supposedly finalized write.
- This is a correctness-shaped gap, not a missing feature — the plan's
  published guarantee is stronger than the code's actual behavior. Must be
  audited against every replication path and documented (or closed) before
  any external durability claim.
- (Credit: Codex flagged this; verification reading
  `data_server.rs::handle_replicate` confirmed the absence.)

### Raft durability (`plans/consensus/raft-core.md`, `src/consensus/raft_storage.rs`)

- `WendelonLogStore::open()` exists but WAL recovery/replay is not implemented.
  `node.rs::bootstrap_single_node` uses `WendelonLogStore::in_memory()`. A
  restart loses every committed entry.
- Checkpoint records (`record_type=3` for `commit_index` and `e_safe_index`)
  per the plan's file format are not written.
- Per-record integrity uses a blake3 prefix; the plan specifies CRC32c. This
  is a wire-format drift, not just a hash-choice preference.
- Snapshot install path not tested end-to-end; `SnapshotData = Cursor<Vec<u8>>`
  is correct but there is no compression / chunking task.

### Schemas (`plans/schema/schemas.md`)

- `RaftEpochEntryFixed.prev_epoch_hash` chain is not enforced on apply; a
  replay with a broken chain would still apply.
- `LogEntryKind::CONFIG` routing path is absent in `apply_request`.
- `TransferToken` is defined but Ed25519 sign/verify functions are not wired.
- `ClientWriteId` is used for idempotency but no explicit TTL enforcement
  against the cache's `last_seen_hlc`.

### Transport (ADR 011, `src/network/transport.rs`)

- Tier 0 RDMA (`RdmaRoceV2`) provider absent. `PoolRegistrar` trait exists;
  the RDMA-pinning hook is a no-op.
- Tier 2 TCP+TLS fallback path is unverified — only the QUIC path is
  exercised in tests.
- Negotiation returns the highest-mutually-supported tier, but capability
  probing timeout / fallback-on-handshake-failure is not implemented.
- No per-peer transport metrics (bytes in/out, retrans, handshake failures).
- mTLS identity: `src/network/tls.rs` comments explicitly flag that SAN
  extraction / `NodeId` verification beyond webpki's standard path is not
  implemented. A presented-and-chain-valid cert is trusted regardless of
  whether its SAN matches the expected peer `NodeId`, so the trust boundary
  the security plan assumes is not yet enforced. (Credit: Codex flagged.)
- `src/network/server.rs` peer-identity plumbing is placeholder — the
  node-id threaded through accept() comes from configuration rather than
  the verified cert chain.

### Ops (`plans/ops/*`)

- `src/main.rs` is a 3-arg dev CLI (node_id, raft_addr, data_addr). No
  `wendelon.toml` loader per `plans/ops/configuration.md`.
- No multi-node join path; no bootstrap-from-peer flow; no seed-list
  handling.
- No Prometheus exporter wiring despite the `observability.md` spec; counter
  names exist only in comments.
- No backup/DR path — no snapshot export, no point-in-time restore
  (`plans/ops/backup_dr.md` entirely unimplemented).
- No rolling-upgrade path; `plans/ops/upgrades.md` specifies a version
  negotiation frame that does not exist.
- No lifecycle hooks (`plans/ops/lifecycle.md`) for graceful drain before
  shutdown.
- No multi-tenancy primitives (`plans/ops/multi_tenancy.md`).

### Tiering (ADR 006)

**Done**
- `SlabSweeper` with `SweeperConfig` (scan_interval, min_slab_bytes,
  max_upload_bytes_per_sec, key_prefix).
- `TierCommitter` + `RaftCommitter` for promotion.
- Two-phase GC via `TierObserver` — `SlabManager` unlinks L1 only after the
  L2 pointer is durably applied.
- `TierSlab` Raft entry committed and applied.

**Gaps**
- `storage/l2.rs::ObjectStore` trait present; no concrete S3/GCS/Azure
  implementation in tree. Tests use an in-memory fake.
- No read-path metrics for L2 cache-miss-then-hit latency.
- No rehydration path (L2 → L1) — a hot slab that was demoted will be read
  from L2 every time.

---

## 3. Suggested sequencing

Ranked by blast radius / dependency order. Anything upstream blocks everything
downstream. Revised to lead with the three correctness-shaped gaps (durability,
recovery, trust boundary) ahead of feature work.

1. **fsync-before-quorum in the replication path** (cross-cutting). This is a
   published-guarantee-vs-actual-behavior mismatch; everything else is
   features on top of a weaker substrate. Add a replica-crash Jepsen-style
   test that asserts acknowledged writes survive.
2. **Raft WAL recovery** (cross-cutting). Without it every committed entry
   is ephemeral. Gate on `tests/raft_recovery.rs` that kills + restarts the
   node and asserts the ledger survives.
3. **TLS SAN → NodeId verification**. A one-afternoon change that closes the
   most impactful security gap. Without it, mTLS provides encryption but
   not authentication.
4. **Control Ring server-side `impl`** (`network/traits.rs::ControlRing`).
   Unblocks 013 subscribe, 017 Raft-proposed eviction, 018 TransferToken
   verification, and 019 auto-seed simultaneously.
5. **HRFT ingress for `WRITE_REQ` / `READ_REQ` / `BULK_WRITE_REQ`**. Without
   this the DataRing is reachable only in-process — the whole client SDK
   story (013) is blocked on this.
6. **Config loader + multi-node bootstrap** (`plans/ops/configuration.md`,
   `plans/ops/lifecycle.md`). Required before any of 017/018/019 can be
   exercised on real hardware.
7. **Epoch-transition auto-seed for Plan 019**. The ledger reconciliation is
   already the hard part; the missing piece is a ~20-line hook in the
   Control Ring apply path, which depends on step 4.
8. **MigrationSweeper background task** (019 step 7–9).
9. **Lease Ed25519 signing + TransferToken verification** (018).
10. **Streaming WPBC responses + client streaming consumer** (015 + 013).
11. **Topology subscribe stream on the client** (013).
12. **Concrete S3 `ObjectStore`** (ADR 006).
13. **Tombstone-aware compaction** (019 step 10).
14. **Prometheus exporter + lifecycle hooks** (ops).
15. **RDMA Tier 0 provider** (ADR 011) — lowest priority; QUIC carries today's
    traffic.

---

## 4. Items to reconcile in the plans themselves

These are places where the plan and the code disagree on a parameter; either
the plan or the code should move:

- `VNODES_PER_NODE`: code says 64, `plans/network/topology.md` says 256.
- WAL per-record hash: code uses blake3-prefix, `plans/consensus/raft-core.md`
  says CRC32c.
- `plans/019_lazy_migration.md` pre-dates the descendant-reconciliation logic
  now in `apply_migration_complete`; the plan should be updated to describe
  the "prepend vs replace" rule the code implements.

---

## 5. Not assessed

- Performance vs. the targets in `plans/ops/benchmarks.md`. No benchmark run
  today; the code compiles but the numbers behind "100 ns-scale hot path" are
  unverified.
- Security posture beyond crypto wiring — no fuzz harness, no threat-model
  review of the HRFT frame parser.
- Correctness under partition / split-brain. The autonomous-eviction path
  (017) is the closest guard; no Jepsen-style test exists.
