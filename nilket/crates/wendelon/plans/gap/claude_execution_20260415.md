# Wendelon Gap-Filling Execution Plan — 2026-04-15

Author: Claude (Opus 4.6)
Source inventory: `plans/gap/claude_assessment_20260415.md`,
`plans/gap/codex_assessment_20260415.md`.

This is a rolling execution plan. Each item has a scope, a definition of
done (DoD), a test gate that proves it's closed, and an explicit dependency
list. Items are grouped into workstreams so multiple can proceed in parallel;
within a workstream the order is prescribed.

**How to use this document:**
- Each item is sized as **S** (≤1 PR, ≤2 days), **M** (2–5 days, possibly
  split PRs), or **L** (requires a design pass before a PR — promote to its
  own `plans/NNN_*.md` first).
- DoD must be falsifiable. "Works correctly" is not a DoD; "restart survives
  1000 committed entries under fault-injection" is.
- Items marked **DESIGN FIRST** should not be started as code — write the
  mini-plan listed in §A0 first, get alignment, then come back here.

**Revision note (2026-04-15b):** folded in three structural pieces from
Codex's parallel execution doc (`codex_execution_20260415.md`) — an
explicit Phase 0 / alignment wave, a work-packet handoff template, and a
program-level exit condition.

---

## 0. Sequencing overview

Seven workstreams, preceded by a short alignment phase. Workstream **A**
(correctness) gates public claims about durability and must finish first.
**B**, **C**, **D** can run in parallel after A.1. **E** and **F** depend on
**B**. **G** is continuous and runs alongside everything.

```
A0. Alignment (mini-plans + drift dispositions)  ─▶  all workstreams
A.  Correctness foundations                      ─┬─▶  everything else
B.  Control-plane surface                        ─┼─▶  D, E
C.  Data-plane ingress                           ─┤
D.  Migration automation                         ─┘
E.  Security / authenticity                      (depends on B)
F.  Storage & tiering                            (parallel to B/C/D)
G.  Ops & observability                          (parallel, continuous)
```

---

## Workstream A0 — Alignment (pre-execution)

A small amount of up-front planning that prevents large rework later. Every
item here produces a short markdown artifact, not code. Target: 1–3 days of
planning work before A.1 starts. (Framing credit: Codex.)

### A0.1 Mini-plan: fsync-before-quorum semantics

Feeds A.1. Must settle:
- Exactly which append paths force durability before ack (WAL, slab, both?).
- Per-write fsync vs group commit vs other explicit boundary.
- How the local leg and remote legs align semantically on the leader.
- Crash tests that prove the guarantee, and their failure models.

Artifact: `plans/020_durability_boundary.md`.

### A0.2 Mini-plan: WAL recovery + bootstrap contract

Feeds A.2. Must settle:
- What `WendelonLogStore::open()` guarantees on restart.
- Where replay ends and checkpoint/snapshot takes over.
- How `node.rs` chooses between fresh bootstrap and recovery.
- Compatibility with any existing on-disk state from dev runs.

Artifact: `plans/021_wal_recovery.md`.

### A0.3 Mini-plan: Control Ring server surface

Feeds B.1. Must settle:
- Which RPCs are required in slice 1 vs deferred (probably just `STATUS_REQ`
  + `ACQUIRE_LEASE_REQ` in slice 1).
- Whether one HRFT listener handles both rings or two.
- Authorization model for `advance_epoch` and `transfer_leadership`.
- Idempotency for control-plane calls.

Artifact: `plans/022_control_ring_server.md`.

### A0.4 Mini-plan: SAN → NodeId verification contract

Feeds E.1. Must settle:
- Where the authoritative `NodeId` is encoded in certs (SAN DNS, URI, OID?).
- Exact verification rule at accept and dial.
- Mismatch surfacing (reject at TLS, or accept TLS and reject at app layer).
- Rollout: hard-fail from day one, or feature-flagged with warn-only
  during migration.

Artifact: `plans/023_san_nodeid_binding.md`.

### A0.5 Mini-plan: epoch-transition auto-seed for lazy migration

Feeds D.1. Must settle:
- Where the seeding hook lives (state-machine apply path vs Control Ring RPC).
- What input diff drives `SeedMigration` emission.
- Idempotency across retries and restarts.
- Tests proving chained migrations remain correct when seeding runs
  alongside in-flight migrations.

Artifact: `plans/024_epoch_transition_autoseed.md`.

### A0.6 Plan/code drift dispositions

Non-blocking but should be closed during A0 so the ambiguity doesn't resurface
mid-execution:
- **WAL integrity hash:** blake3 (code) vs CRC32c (plan). Pick one; update
  the other. Likely answer: keep blake3, update `plans/consensus/raft-core.md`.
- **`VNODES_PER_NODE`:** 64 (code) vs 256 (plan). Pick one; update the other.
- **`plans/019_lazy_migration.md`** pre-dates the descendant-reconciliation
  logic now in `apply_migration_complete`. Update the plan to match.

Artifact: edits to the three plan docs named above.

### A0 exit criteria

- Five mini-plans written and agreed.
- Three drift items have a disposition (code change, plan change, or both).
- Workstream ownership assigned (see Open Questions at the bottom).

---

## Workstream A — Correctness foundations

These items change the actual failure semantics of the system. They must land
before any external durability / recovery claim.

### A.1 fsync before quorum ack in the replication path  [S]

- **Source:** assessment §2 "Durability boundary"
- **Scope:** On the replica side, `handle_replicate` must `fsync` the WAL +
  slab append before returning `REPLICATE_RESP` with
  `DurabilityLevel::QUORUM`. On the leader, `client_write` must not count an
  ack toward quorum until the ack carries a durable flag.
- **DoD:**
  1. Every return path in `handle_replicate` that produces a `QUORUM`-level
     response is preceded by a durable-flush call.
  2. The leader's quorum counter in `DataRing::write` only increments on
     acks whose `durable=true`.
  3. A new `DurabilityLevel::APPENDED` ack exists for callers that
     explicitly opt out of fsync.
- **Test gate:** `tests/replication_durability.rs` — chaos test that crashes
  `RF/2 + 1` replicas between WAL append and fsync. Writes that received a
  `QUORUM` ack must be present after restart; writes that did not must be
  absent. Run 100 trials.
- **Dependencies:** none.
- **Owner hint:** storage + network; one author.

### A.2 Raft WAL recovery on node startup  [M]

- **Source:** assessment §2 "Raft durability"; Codex §5.
- **Scope:** Make `WendelonLogStore::open()` replay the WAL on construction;
  switch `node.rs::bootstrap_single_node` from `in_memory()` to the durable
  path. Implement checkpoint records (`record_type=3` for `commit_index` and
  `e_safe_index`) per `plans/consensus/raft-core.md`.
- **DoD:**
  1. Cold-start from an existing WAL reconstructs `SmState` bit-for-bit.
  2. Checkpoint records are emitted at a configurable interval; replay
     starts from the most recent checkpoint.
  3. Truncation at a torn tail record leaves the state machine consistent.
- **Test gate:** `tests/raft_recovery.rs` — write N=1000 entries, kill the
  process, restart, assert every entry applied. Repeat with a random
  truncation of the last record. Repeat with a corrupted checkpoint.
- **Dependencies:** A.1 (because a recovered WAL must match the durability
  story the write path claims).
- **Blocking for:** everything requiring a real multi-node deployment.

### A.3 WAL integrity: reconcile blake3 vs CRC32c  [S]

- **Source:** plan reconciliation §4 in the assessment; disposition made
  in A0.6.
- **Scope:** Apply the decision from A0.6. If code moves to CRC32c: swap
  the per-record hash in `raft_storage.rs` and `storage/io_uring.rs`. If
  plan moves to blake3: edit `plans/consensus/raft-core.md`.
- **DoD:** One hash is used consistently across code and plan.
- **Test gate:** existing tests pass with the chosen hash. Add a torn-write
  detection test that confirms bit-flips are caught.
- **Dependencies:** A0.6, A.2 (change the hash while touching the WAL
  format, not separately).

---

## Workstream B — Control-plane surface

Blocks 013 / 017 / 018 / 019 pieces. The client is already speaking
`STATUS_REQ` into the void; this wires up the listener.

### B.1 Server-side `impl ControlRing for WendelonNode`  [L, mini-plan A0.3]

- **Source:** assessment §2 "Control Ring RPC surface"; Codex §1.
- **Scope:** Per A0.3. Implement the trait methods chosen for slice 1
  (`transfer_leadership`, `acquire_lease`, `advance_epoch`,
  `get_cluster_status`) and route the corresponding HRFT message types
  (`STATUS_REQ`, `ACQUIRE_LEASE_REQ`, `TRANSFER_LEADERSHIP_REQ`,
  `ADVANCE_EPOCH_REQ`) through a new `ControlRingServer` analogous to
  `WendelonDataServer`.
- **DoD:**
  1. `handle_control_frame` routes all four message types.
  2. `get_cluster_status` is served end-to-end; the client's
     `ControlRingTopologySource::refresh` succeeds against a live peer.
  3. `acquire_lease` is reachable from a remote caller and commits through
     Raft.
- **Test gate:** `tests/control_ring_rpc.rs` — smoke-test each RPC against
  a single-node cluster; add a two-node test for `acquire_lease` racing.
- **Dependencies:** A.2 (control-plane calls must survive restart).

### B.2 `WatchMembershipAdvancer` → Raft-proposed membership change  [M]

- **Source:** assessment §1 Target 017.
- **Scope:** Replace the `watch`-channel shortcut in `node.rs` with a
  `ChangeMembership` proposal through Raft. Eviction decisions in
  `sweep_evictions_once` must propose, not locally mutate.
- **DoD:**
  1. Eviction decisions require a Raft quorum commit.
  2. A partitioned minority cannot evict anyone.
  3. The local watchdog still aborts the current process on self-drift —
     that part was correct.
- **Test gate:** three-node cluster; partition one node, assert it cannot
  force a membership change; heal, assert it rejoins. Drift one node's
  HLC past threshold, assert the cluster evicts it via a committed
  `ChangeMembership`.
- **Dependencies:** B.1 (needs a real control-plane RPC to propose from
  an external caller), A.2.

---

## Workstream C — Data-plane ingress

The DataRing trait methods exist; they're just not reachable from the wire.

### C.1 HRFT ingress for `WRITE_REQ` / `READ_REQ`  [M]

- **Source:** assessment Target 012 (expanded), Codex §2.
- **Scope:** Add arms in `data_server.rs::handle_data_frame` for `WRITE_REQ`
  and `READ_REQ` that decode the frame meta + Arrow payload, call
  `DataRing::write` / `read`, and encode the response.
- **DoD:**
  1. A client over QUIC can perform an end-to-end write and read without
     any in-process shortcut.
  2. Error paths (epoch stale, lease expired, not leader, e-safe behind)
     are all exercised with correct `WriteStatus` / `ReadStatus` codes.
- **Test gate:** `tests/data_ring_wire.rs` — two-node cluster, client-side
  `WendelonClient`, round-trip write-then-read, all error codes covered
  via fault injection on the server state.
- **Dependencies:** none (DataRing internals already work).
- **Unblocks:** most of 013.

### C.2 `BULK_WRITE_REQ` ingress + leader-side HLC merge  [M]

- **Source:** assessment Target 012 (bulk + HLC merge).
- **Scope:** Replace the `bulk_write` stub with a real streaming Arrow IPC
  consumer. Apply `hlc_commit = max(leader_hlc, client_hlc) + 1` at the
  leader for both single and bulk paths.
- **DoD:**
  1. A 100 MB bulk write streams without buffering the whole payload.
  2. HLC merge is enforced; a client sending a past HLC gets a merged
     commit HLC, not a silent accept.
  3. Bulk writes participate in the same quorum + durability contract as
     single writes (ties into A.1).
- **Test gate:** `tests/bulk_write.rs` — stream 100 MB, assert memory
  high-water-mark stays below 2× chunk size. Inject a client HLC from the
  past, assert the response carries the merged HLC.
- **Dependencies:** C.1, A.1.

### C.3 `stream_finalization` real implementation  [M]

- **Source:** assessment Target 012.
- **Scope:** Replace the `mpsc::channel(1)` stub. Subscribe to the Raft
  state machine's `FinalizationMarker` stream and push to client.
- **DoD:**
  1. A subscribing client receives every finalization after their chosen
     HLC floor.
  2. Back-pressure from a slow client does not stall the state machine.
  3. Disconnection is survivable — reconnect-with-HLC resumes.
- **Test gate:** `tests/stream_finalization.rs` — drive 10k writes, assert
  client sees all markers in order; slow-drain variant.
- **Dependencies:** C.1.

---

## Workstream D — Migration automation

The state-machine side is done; this closes the automation loop.

### D.1 Epoch-transition auto-seed  [S]

- **Source:** assessment Target 019.
- **Scope:** In the epoch-advance apply path, for each reassigned range
  compute the deterministic `migration_id` and propose `SeedMigration`.
  Plan-doc budget: ~20 lines of code + an entry in
  `plans/019_lazy_migration.md`.
- **DoD:**
  1. Advancing from `ring(E)` to `ring(E+1)` emits one `SeedMigration`
     per reassigned range, idempotent on replay.
  2. Ledger reconciliation for overlapping chained migrations continues
     to pass existing tests.
- **Test gate:** `tests/epoch_transition_seed.rs` — advance an epoch that
  reassigns 10 ranges; assert 10 ledger entries exist. Replay the same
  entry; assert no duplicate.
- **Dependencies:** B.1 (the trigger is a control-plane RPC) or it can be
  driven purely from the state-machine apply path, which is simpler. Pick
  the latter in the design doc.

### D.2 Background `MigrationSweeper` task  [M]

- **Source:** assessment Target 019.
- **Scope:** Per-node task that scans owned ledger entries in `Pending` /
  `BackfillInProgress` state, issues `MIGRATE_REQ` to source replicas,
  ingests via `SlabManager::append_migration`, and proposes
  `MigrationComplete` on drain.
- **DoD:**
  1. A seeded ledger entry drains without external intervention.
  2. Backoff + source failover on `INTERNAL_ERROR` works.
  3. Rate-limiting token bucket prevents migration from saturating the
     transport.
- **Test gate:** `tests/migration_sweeper.rs` — seed an entry, wait,
  assert `state == Completed`. Kill the preferred source mid-drain,
  assert the sweeper fails over to the next candidate.
- **Dependencies:** D.1, C.1.

### D.3 Tombstone-aware compaction (step 10)  [M]

- **Source:** assessment Target 019.
- **Scope:** Slab rewrite path that drops versions covered by a
  `MigratedRangeTombstone` whose floor is below the tombstone's
  `migration_hlc_floor`. Must coordinate with the tier sweeper to avoid
  rewriting a slab that's mid-promotion.
- **DoD:**
  1. A post-migration slab rewrite reclaims bytes for ranges that moved
     away.
  2. Live versions for ranges still owned locally are preserved.
  3. Time-travel reads at `as_of < tombstone.floor` still succeed from
     the new physical owner, not the rewritten slab.
- **Test gate:** `tests/tombstone_compaction.rs` — migrate range, trigger
  rewrite, assert bytes reclaimed and `lookup_as_of` correctness.
- **Dependencies:** D.2.

### D.4 Client `MIGRATION_INCOMPLETE` retry  [S]

- **Source:** assessment Target 019.
- **Scope:** Client-side backoff + retry on `MIGRATION_INCOMPLETE`; surface
  final failure only after N attempts.
- **DoD:** A read against a range mid-migration completes without the
  caller seeing the incomplete state, as long as it resolves before the
  retry budget.
- **Test gate:** `tests/client_migration_retry.rs` — induce
  `MIGRATION_INCOMPLETE`, assert client transparently retries.
- **Dependencies:** C.1.

---

## Workstream E — Security / authenticity

### E.1 TLS SAN → NodeId binding  [S]

- **Source:** assessment §2 "Transport"; Codex §8.
- **Scope:** Post-handshake, extract SAN from peer cert and assert it
  matches the expected `NodeId`. Reject otherwise. Done in
  `src/network/tls.rs` and plumbed through `server.rs::accept`.
- **DoD:**
  1. A cert with a mismatching SAN is rejected.
  2. Accepted connections carry a verified `NodeId` that downstream code
     can trust instead of re-reading from frame metadata.
- **Test gate:** `tests/tls_identity.rs` — happy path, SAN mismatch,
  missing SAN, expired cert.
- **Dependencies:** none.

### E.2 Ed25519 lease signing + verification  [M]

- **Source:** assessment Target 018; Codex §8.
- **Scope:** Replace zero-filled `LeaseDescriptor.signature` with a real
  Ed25519 signature by the issuing leader. Verify on use in
  `has_valid_lease` and on apply in `raft_storage`.
- **DoD:**
  1. A forged `LeaseDescriptor` cannot pass `has_valid_lease`.
  2. Leadership keys are loaded from config (plumbs into F.1).
  3. Key rotation path exists (schema bump or key-id field).
- **Test gate:** `tests/lease_signing.rs` — valid sign/verify,
  flipped-bit signature rejected, wrong-key signature rejected.
- **Dependencies:** F.1 (config loader) for key material.

### E.3 `TransferToken` Ed25519 enforcement  [S]

- **Source:** assessment §2 "Schemas".
- **Scope:** Wire sign/verify on `TransferToken` in the leadership-transfer
  path.
- **DoD:** A `TransferToken` without a valid leader signature is rejected.
- **Test gate:** extends `tests/lease_signing.rs`.
- **Dependencies:** E.2, B.1.

---

## Workstream F — Storage & tiering

### F.1 `wendelon.toml` config loader + multi-node bootstrap  [M]

- **Source:** assessment §2 "Ops"; Codex §11.
- **Scope:** Replace the 3-arg CLI in `main.rs` with a `wendelon.toml`
  loader per `plans/ops/configuration.md`. Add seed-list handling and a
  bootstrap-from-peer flow.
- **DoD:**
  1. A 3-node cluster can be brought up from config files alone.
  2. Node identity (including TLS key, Ed25519 lease key) is loaded
     from config.
  3. Join-existing-cluster flow works end-to-end.
- **Test gate:** `tests/multinode_bootstrap.rs` — spin 3 nodes, assert
  Raft leader elected, assert `get_cluster_status` reflects membership.
- **Dependencies:** A.2, B.1.

### F.2 Concrete S3 `ObjectStore` implementation  [M]

- **Source:** assessment Target tiering.
- **Scope:** Real S3-backed `ObjectStore` with put / get / delete and
  retry + rate-limit. Tests run against RustFS (see note below).
- **DoD:** `SlabSweeper` uploads to S3 end-to-end; `read_from_l2`
  fetches.
- **Test gate:** `tests/l2_s3.rs` — round-trip against a local RustFS
  instance started by the test harness.
- **Test-dependency note:** RustFS was chosen over MinIO — see
  `plans/adr/014_rustfs_test_backend.md` for context and revisit
  triggers.
- **Dependencies:** none.

### F.3 L2 → L1 rehydration for hot slabs  [M, mini-plan required]

- **Source:** assessment Target tiering.
- **Scope:** Policy for promoting a frequently-read L2 slab back to L1.
- **Mini-plan must settle:** heuristic choice (LRU, hit-rate threshold,
  adaptive), eviction policy on the promoted L1 side, thrash guards.
  Artifact: `plans/025_l2_rehydration.md`. Not part of A0 because it's
  not on the critical path.
- **Dependencies:** F.2.

---

## Workstream G — Ops & observability (continuous)

Runs alongside everything. Treat as a background workstream, not a phase.

### G.1 Prometheus exporter + core metrics  [S]

- **Source:** assessment §2 "Ops"; `plans/ops/observability.md`.
- **Scope:** Expose counters named in `observability.md`: quorum latency,
  fsync latency, migration sweeper throughput, HLC drift aborts, L2
  cache miss rate, per-peer transport bytes.
- **DoD:** `/metrics` endpoint scraped by Prometheus returns all named
  counters.
- **Test gate:** integration test that scrapes and asserts presence.
- **Dependencies:** none, but should land before any other M-sized item
  so we have telemetry through the build-out.

### G.2 Graceful drain / lifecycle hooks  [S]

- **Scope:** Shutdown sequence that stops accepting writes, drains
  in-flight work, steps down if leader, closes sockets.
- **DoD:** SIGTERM produces a clean shutdown with no dropped writes.
- **Test gate:** `tests/graceful_shutdown.rs`.
- **Dependencies:** none.

### G.3 Rolling upgrade protocol  [L, mini-plan required]

- **Source:** `plans/ops/upgrades.md`.
- **Scope:** Version negotiation frame, schema-compat rules.
- **Mini-plan must settle:** wire-format compat matrix, skew window
  (how many versions back we accept), rollback procedure. Artifact:
  `plans/026_rolling_upgrade.md`. Out of A0 scope.
- **Dependencies:** F.1.

### G.4 Backup / DR  [L, mini-plan required]

- **Source:** `plans/ops/backup_dr.md`.
- **Mini-plan must settle:** point-in-time recovery target, export format
  (raw slabs + Raft snapshot, or a higher-level dump), restore-into-new-cluster
  flow. Artifact: `plans/027_backup_dr.md`. Out of A0 scope.
- **Dependencies:** F.2.

---

## Out of scope for this plan

Items from the assessment deferred to a later execution wave:

- **Streaming WPBC responses + client streaming consumer.** Depends on
  C.1 and changes the client API shape; better to bundle with the full
  DataFusion `TableProvider` work.
- **DataFusion / PGWire adapters.** Target 013 non-critical — park until
  the core SDK is usable.
- **RDMA Tier 0 provider.** Deliberately last; QUIC carries today's
  traffic.
- **Topology subscribe stream (push).** Can stay pull-only until a real
  operator asks for it.
- **Per-range rack/AZ anti-affinity.** Target 016 nice-to-have.
- **VNODES_PER_NODE reconciliation.** Plan-doc edit, trivial, bundle
  with any ring change.

---

## Review cadence

- Revisit this plan every time a workstream closes an item (update
  status, re-check dependencies).
- Promote any `mini-plan required` item to `plans/NNN_*.md` before
  writing code against it.
- If two items collide on the same file (e.g. C.2 and A.1 both touch
  `handle_replicate`), sequence them explicitly rather than merging
  the PRs.

## Work packet format

When handing an item from this plan to an implementer (human or agent),
spell the work out in this shape. Narrative instructions like "close the
gaps in plan 012" are not specific enough to act on. (Template credit: Codex.)

```
Objective:          <one sentence from the item's Scope>
Item id:            <e.g. A.1>
Files in scope:     <exact paths expected to change>
Invariants to add:  <what must become true about the system>
Non-goals:          <what this packet explicitly does not address>
Test gate:          <test file path + what it asserts>
Closes:             <assessment section + execution item>
Size:               <S / M / L>
Depends on:         <items that must be done first>
```

A packet that omits "invariants to add" or "non-goals" will drift; require
both before accepting the packet as ready to execute.

## Exit condition for the gap-closure program

This program is complete (and the remaining work is normal product
hardening, not gap closure) when all of the following are true
simultaneously. (Framing credit: Codex.)

- The published durability and recovery guarantees in `plans/` are true
  in code. Specifically: acknowledged writes survive `RF/2 + 1` replica
  crashes; a restarted node rehydrates committed state from the WAL.
- Control-plane and data-plane behaviors are reachable over the wire
  from a separate process, not only through in-process test shortcuts.
- The client drives the system over those wire surfaces using the same
  retry, topology, and migration semantics a real operator would see.
- Migrations seed, drain, and reclaim automatically after an epoch
  change, with no manual or placeholder steps in the path.
- Lease and leadership-transfer authenticity is cryptographically
  enforced, and peer identity is bound to TLS.
- A cluster can be configured from `wendelon.toml` and brought up from
  config alone, with core metrics exposed and a graceful-drain path.

Tiering backends, rolling-upgrade protocol, DataFusion integration, and
RDMA can remain open after this point — they are product/hardening work,
not architectural debt.

## Open questions for the user

1. Who owns each workstream? Items are marked owner-free today.
2. Are we targeting a specific release milestone? The sequencing
   assumes "ship when A+B+C+E are closed" — explicit would help.
3. ~~Is MinIO an acceptable test dependency for F.2?~~ **Resolved
   2026-04-15:** use RustFS instead — MinIO went closed-source; RustFS
   is alpha but has S3-layer parity and a 1.0 target three months out.
