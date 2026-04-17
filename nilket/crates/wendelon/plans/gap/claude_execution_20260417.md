# Wendelon Execution Plan — Gap Closure to Production Readiness

**Date:** 2026-04-17
**Derived from:** `plans/gap/claude_assessment_20260417.md`
**Peer inputs incorporated:** Codex execution plan (2026-04-17), Gemini execution plan (2026-04-17)
**Status:** Skeletal / iteration 2 — detail passes to follow

---

## Guiding Principles

1. **Integration gate first.** The single highest-leverage item is a
   real-network smoke test that runs on every PR. Most P0 gaps are wiring
   omissions that would have been caught by one. Build the gate before
   closing the gaps so that every subsequent fix is verified end-to-end.
2. **Stock runtime path is the definition of done.** Do not treat a
   subsystem as done until it is exercised through the shipped daemon
   path — not a test-only embedding, not a library call, not a manually
   constructed provider. If `main.rs` doesn't use it, it doesn't count.
   *(Adopted from Codex.)*
3. **Correctness before features.** Durability lifecycle completion and
   security fixes before new capabilities.
4. **Small, independently verifiable units.** Each work item should
   produce a commit (or small PR) that either passes or fails the
   integration gate. No multi-week branches.
5. **Fix the footgun, then harden.** One-line fixes (TierObserver wiring,
   JOIN identity binding) ship immediately. Design-heavy items (encryption
   at rest, RBAC) get an ADR first.
6. **Every phase lands with tests, docs, and operator-facing behavior.**
   Internal code alone does not close a phase. *(Adopted from Codex.)*

---

## Phase 0 — Integration Gate (prerequisite to all other phases)

**Goal:** A CI-runnable test that proves the production binary can form a
cluster, accept writes, read them back, and survive a node restart.

**Why first:** Nearly every other blocker is easier to close once the
actual runtime path exists and can be tested end-to-end. The three
independent gap reviews all converged on this: the dominant risk is wiring
gaps invisible to unit tests. *(Codex: "Phase 1 unlocks almost everything
else." Gemini led with this as Workstream 1.B.)*

| # | Work Item | Target Files | Depends On | Scope |
|---|-----------|-------------|------------|-------|
| 0.1 | Wire `config.rs` to load TLS material (`cert_path`, `key_path`, `ca_path`) from TOML | `src/config.rs` | — | config |
| 0.2 | Wire `config.rs` to load L2 config from TOML (remove `l2: None` hardcode at line 231) | `src/config.rs` | — | config |
| 0.3 | Wire `main.rs` to construct `quinn::Endpoint`, call `serve_quic` | `src/main.rs`, `src/node.rs` | 0.1 | daemon |
| 0.4 | Wire `main.rs` to support `cluster.mode = "join"` via real QUIC provider (remove `ErrorKind::Unsupported` gate at `node.rs:379`) | `src/node.rs`, `src/main.rs` | 0.3 | daemon |
| 0.5 | Install tracing subscriber in `main.rs` (currently no subscriber means all `tracing::*!` macros are no-ops) | `src/main.rs` | — | daemon |
| 0.6 | Integration test harness: 3 nodes, real `quinn::Endpoint` + mTLS, exercises JOIN, WRITE, READ, node restart | `tests/integration_smoke.rs` (new) | 0.3, 0.4 | test |
| 0.7 | CI job that runs the integration test on every PR | CI config | 0.6 | infra |

**Exit criteria:** `cargo test --test integration_smoke` passes with 3
real QUIC nodes forming a cluster, writing data, reading it back, and
surviving one node restart.

---

## Phase 1 — Critical Correctness Fixes

**Goal:** Fix bugs and wiring omissions that cause data loss, disk leaks,
or security vulnerabilities. All items are small and independently
shippable.

**Why here:** These are the cheapest fixes with the highest blast radius.
Each is a day or less of work. Several were independently flagged by
multiple reviewers. Ship them immediately so every subsequent phase builds
on a sound foundation.

| # | Work Item | Target Files | Depends On | Scope |
|---|-----------|-------------|------------|-------|
| 1.1 | Fix JOIN_REQ identity binding: assert `req.node_id == remote` in `accept_join` (`control_server.rs:937`) | `src/network/control_server.rs` | — | security |
| 1.2 | Negative test: malicious node attempts JOIN with a `node_id` that doesn't match its certificate SAN — must be rejected | `tests/` | 1.1 | security |
| 1.3 | Wire `ManagerTierObserver` into `node.rs` bootstrap path (call `with_tier_observer` on the log store) | `src/node.rs` | — | storage |
| 1.4 | Add dev-key warning to `render_example_config` output | `src/config.rs` | — | security |
| 1.5 | Add idempotency cache TTL sweep task (replace unbounded `DashMap` insert-only pattern) | `src/network/data_server.rs` | — | memory |
| 1.6 | Bound per-peer QUIC stream concurrency (`server.rs:423` spawn loop) | `src/network/server.rs` | — | memory |

**Exit criteria:** Each fix has a targeted test. Items 1.1 and 1.3
verified by the Phase 0 integration gate. The malicious-join negative
test (1.2) proves the identity binding is enforced. *(Negative test idea
from Gemini.)*

---

## Phase 2 — Raft Durability Lifecycle Completion

**Goal:** The Raft subsystem persists snapshots, emits checkpoints, and
compacts the WAL durably — not just in memory.

**Why here:** Runtime bring-up and critical fixes land first, but
durability lifecycle is the next correctness boundary. Without it, WAL
growth is unbounded and recovery depends on artifacts that are never
produced in steady-state operation. *(Codex: "Recovery artifacts are
produced in normal operation, not just consumed on boot.")*

| # | Work Item | Target Files | Depends On | Scope |
|---|-----------|-------------|------------|-------|
| 2.1 | Persist `snapshot.wnd` to disk in `install_snapshot` and `build_snapshot` | `src/consensus/raft_storage.rs:1008`, `:1309` | — | consensus |
| 2.2 | Emit `REC_CHECKPOINT` records at runtime (periodic or on snapshot) | `src/consensus/raft_storage.rs` | 2.1 | consensus |
| 2.3 | Implement durable WAL purge/compaction (rewrite or hole-punch) | `src/consensus/raft_storage.rs:677`, `:697` | 2.2 | consensus |
| 2.4 | Large-snapshot handling: chunked/streamed install instead of `Cursor<Vec<u8>>` | `src/consensus/raft_storage.rs:1002` | 2.1 | consensus |
| 2.5 | Recovery test: crash after snapshot persist, restart recovers from snapshot + WAL tail | `tests/` | 2.1 | test |
| 2.6 | Recovery test: WAL compaction followed by crash, restart uses compacted WAL | `tests/` | 2.3 | test |

**Exit criteria:** WAL size is bounded in a long-running test.
`snapshot.wnd` exists on disk after snapshot. Recovery from
snapshot + tail is tested.

---

## Phase 3 — L2 Tiering Hardening

**Goal:** Object storage tiering is crash-safe, leak-free, and wired into
the production daemon.

**Why here:** Tiering shares many of the same state-lifecycle concerns as
Phase 2 and should be closed against a stronger durability foundation. The
follower disk leak (1.3) is the immediate fix, but the deeper crash-safety
and reconciliation issues need the integration gate and durability work as
prerequisites. *(Codex: "Tiering closure can overlap with durability
closure, but only if ownership of shared persistence/recovery invariants
is clear.")*

**Note on trait-breaking change:** Item 3.7 (range-GET) changes the
`ObjectStore` trait shape. Gemini placed this in their Phase 2, and the
argument is sound — do the breaking change early before more code depends
on the current three-verb trait. Consider pulling 3.7 forward or batching
it with streaming put, multipart, and list in one break (see Open
Question #5).

| # | Work Item | Depends On | Scope |
|---|-----------|------------|-------|
| 3.1 | Wire `SlabSweeper` into `node.rs` bootstrap (not just migration sweeper) | Phase 0 | storage |
| 3.2 | Pending-upload journal for crash-safe L1->L2 transitions | — | storage |
| 3.3 | Orphan GC: reconcile `tiered_slabs` Raft state against bucket contents | 3.2 | storage |
| 3.4 | Crash-safe local compaction: journal old->new slab swap | — | storage |
| 3.5 | Follower-side tiered-slab reconciliation on restart | 1.2 | storage |
| 3.6 | L2 object deletion lifecycle (retention expiry, migration cleanup) | — | storage |
| 3.7 | Range-GET support in `ObjectStore` trait (breaking change) | — | storage |
| 3.8 | Multipart upload for large slabs | 3.7 | storage |
| 3.9 | Integration test: tier a slab, crash, restart, verify no orphans or leaks | 3.2, 3.3 | test |

**Exit criteria:** A multi-node integration test that tiers data to a
real (or MinIO) S3 endpoint, crashes a node mid-tier, restarts, and
verifies: no orphaned S3 objects, no leaked local files, data readable.

---

## Phase 4 — Encryption at Rest

**Goal:** All data at rest is encrypted. KMS-integrated key management
with per-tenant key support.

**Why here:** This is an enterprise gating item — every reviewer flagged
it as the single most disqualifying gap for regulated industries. Codex
argued for placing security immediately after runtime bring-up ("should
not wait behind later scaling or UX work"). This phase can and should
start in parallel with Phases 2–3; the ADR (4.0) has no code
dependencies.

**Note on io_uring alignment:** Encryption must integrate cleanly with
the `O_DIRECT` + `AlignedBuf` write path. The ADR must address whether
encryption happens before or after page alignment and how it interacts
with the io_uring submission queue. *(Gemini flagged the injection point
as `src/storage/core.rs` append path.)*

| # | Work Item | Depends On | Scope |
|---|-----------|------------|-------|
| 4.0 | ADR: encryption-at-rest design (envelope scheme, KMS interface, key hierarchy, rotation, crypto-erase) | — | design |
| 4.1 | KMS trait + implementations (AWS KMS, GCP KMS, Vault Transit, local-dev) | 4.0 | crypto |
| 4.2 | Slab record encryption (DEK per slab, KEK in KMS) | 4.1 | storage |
| 4.3 | WAL page encryption | 4.1 | consensus |
| 4.4 | Snapshot encryption | 4.1 | consensus |
| 4.5 | L2 object encryption (client-side envelope or SSE-KMS) | 4.1 | storage |
| 4.6 | Key rotation support (re-wrap DEKs with new KEK, no data rewrite) | 4.2 | crypto |
| 4.7 | Per-tenant key isolation (prep for Phase 6 multi-tenancy) | 4.2 | crypto |
| 4.8 | Integration test: encrypted write, restart, read back; key rotation; crypto-erase | 4.2–4.5 | test |

**Exit criteria:** `grep` for plaintext Arrow payloads on slab files
returns nothing. Key rotation test passes. Encrypted data survives
node restart.

---

## Phase 5 — Security and Compliance

**Goal:** Authorization, audit, cert lifecycle, and hardened operational
surface.

**Why here:** Codex grouped encryption + authz + audit into a single
"security floor" phase. That has merit for enterprise gating speed, but
I've kept them separate because encryption (Phase 4) is design-heavy with
a KMS integration dependency, while authz/audit are more about policy
enforcement on existing RPC surfaces. Both can run in parallel. The audit
stream (5.2) has no dependencies and could start as early as Phase 1.

| # | Work Item | Depends On | Scope |
|---|-----------|------------|-------|
| 5.0 | ADR: authorization model (role hierarchy, cert OID vs. separate CA, scope claims) | — | design |
| 5.1 | Control Ring authorization: distinguish operator vs. data-node identity | 5.0 | security |
| 5.2 | Audit log stream: append-only NDJSON per node for admin actions | — | compliance |
| 5.3 | Hot cert rotation (file watcher or SIGHUP reload for `TlsMaterial`) | — | security |
| 5.4 | Ed25519 key rotation: add key-id field to `LeaseDescriptor`, overlap window | — | security |
| 5.5 | FIPS mode: swap `ring` for `aws-lc-rs` behind feature flag | — | compliance |
| 5.6 | Metrics endpoint auth + TLS | — | security |
| 5.7 | Rate limiting / ban list for malformed HRFT frames | — | security |
| 5.8 | Secret redaction policy for tracing spans | — | security |

**Exit criteria:** An operator cert can perform admin RPCs; a data-node
cert cannot. Audit log captures all membership changes and lease grants.
Cert rotation without cluster restart.

---

## Phase 6 — Observability and Operations

**Goal:** Enterprise-grade operational surface for SRE teams.

**Why here:** Observability design can start early — Codex noted this
explicitly. The tracing subscriber (Phase 0, item 0.5) is the minimal
baseline; this phase builds the full SRE surface. The operator CLI (6.4)
depends on the runtime being real (Phase 0) but otherwise has no
blockers. Per Codex's principle: "the system must be operable by someone
other than the implementation team."

| # | Work Item | Depends On | Scope |
|---|-----------|------------|-------|
| 6.1 | Distributed tracing: OTLP exporter, span propagation across QUIC RPCs | — | observability |
| 6.2 | Latency histograms + dimensional labels in metrics registry | — | observability |
| 6.3 | OpenMetrics / remote-write export option | — | observability |
| 6.4 | Operator CLI: `wendelon backup`, `restore`, `cluster-status`, `transfer-leader`, `drain` | Phase 0 | tooling |
| 6.5 | Health endpoint + k8s readiness/liveness probes | — | operations |
| 6.6 | Systemd notify / container lifecycle integration | — | operations |
| 6.7 | Tune openraft config defaults (election, heartbeat, snapshot, compaction) with documented rationale | — | operations |

**Exit criteria:** A write can be traced across leader + 2 replicas in
Jaeger/Tempo. `wendelon cluster-status` returns node health, epoch, and
leader. k8s deployment with rolling upgrade succeeds.

---

## Phase 7 — Fault Injection and Correctness Proving

**Goal:** Jepsen-grade confidence that the system is linearizable under
partition and crash.

**Why here:** The harness (7.1) can be built in parallel with earlier
phases, but the test scenarios are most valuable once durability (Phase 2)
and tiering (Phase 3) are complete — there's more surface area to break.
Codex folded fault injection into their resource-control phase; I keep it
separate because it's a distinct skill set and the "fix whatever it finds"
tail (7.9) is the unbounded part of the entire plan.

| # | Work Item | Depends On | Scope |
|---|-----------|------------|-------|
| 7.1 | Deterministic network simulator harness (madsim or turmoil) | — | test |
| 7.2 | Test: majority partition survives, minority rejects writes | 7.1 | test |
| 7.3 | Test: rejoining minority catches up | 7.1 | test |
| 7.4 | Test: single-node crash during `append_durable` (between WAL and slab) does not lose acked write | 7.1 | test |
| 7.5 | Test: quorum write with one crash during replication replays on recovery | 7.1 | test |
| 7.6 | Test: leader crash during snapshot install, new leader elected | 7.1 | test |
| 7.7 | Test: L1->L2 tier crash at each step of the two-phase pivot | 7.1 | test |
| 7.8 | Long-running soak: hours of logical time, random partitions, random crashes, verify linearizability | 7.1–7.7 | test |
| 7.9 | Fix whatever 7.1–7.8 find | 7.8 | varies |

**Exit criteria:** Soak test runs for 10,000+ logical operations under
random fault injection with zero linearizability violations.

---

## Phase 8 — Performance and Scale Hardening

**Goal:** Bounded memory, predictable latency, and fast restart at
enterprise data volumes.

**Why here:** Once the product path is real, secure, and durable, the
next risk is operational instability under realistic pressure. *(Codex
phrasing, adopted.)*

| # | Work Item | Depends On | Scope |
|---|-----------|------------|-------|
| 8.1 | Global memory budget / backpressure framework | — | runtime |
| 8.2 | Bounded Arrow buffers on read path (streaming query response) | — | query |
| 8.3 | Group commit on WAL (fsync coalescer) | — | consensus |
| 8.4 | Snapshot chunking + compression | Phase 2 | consensus |
| 8.5 | Persistent slab index (avoid full rescan on restart) | — | storage |
| 8.6 | Background scrubber for L1 and L2 with manifest hashes | — | storage |
| 8.7 | Benchmark suite: quorum write throughput, p99 latency, restart time vs. data volume | — | test |
| 8.8 | NUMA pinning / CPU affinity for io_uring thread | — | runtime |

**Exit criteria:** Restart time sub-linear in data volume. OOM test
(pathological client) triggers backpressure, not crash. Benchmark
numbers documented.

---

## Phase 9 — Multi-Tenancy

**Goal:** Per-tenant namespaces, quotas, routing, and key isolation.

| # | Work Item | Depends On | Scope |
|---|-----------|------------|-------|
| 9.0 | ADR: multi-tenancy design (namespace scheme, routing, quota enforcement, key isolation) | Phase 4, Phase 5 | design |
| 9.1 | Namespace-prefixed schema and ring routing | 9.0 | schema |
| 9.2 | Per-namespace lease grants | 9.0 | consensus |
| 9.3 | Per-tenant quotas (storage, throughput) | 9.0 | runtime |
| 9.4 | Per-tenant L2 bucket routing | 9.0 | storage |
| 9.5 | Per-tenant encryption keys (wired to Phase 4 KMS) | 9.0, 4.7 | crypto |
| 9.6 | Integration test: two tenants, isolated writes/reads, quota enforcement | 9.1–9.5 | test |

**Exit criteria:** Two tenants on the same cluster cannot read each
other's data. Quota breach returns a clear error, not OOM.

---

## Phase Dependency Graph

```
Phase 0  (integration gate)
  |
  +---> Phase 1  (critical fixes)
  |       |
  |       +---> Phase 2  (Raft durability)
  |       |       |
  |       +---> Phase 3  (L2 hardening)
  |       |
  |       +---> Phase 7  (fault injection) -- can start after 2 & 3
  |
  +---> Phase 4  (encryption at rest) -- can start in parallel with 1-3
  |       |
  |       +---> Phase 9  (multi-tenancy) -- needs 4 + 5
  |
  +---> Phase 5  (security/compliance) -- can start in parallel with 1-3
  |       |
  |       +---> Phase 9
  |
  +---> Phase 6  (observability/ops) -- can start anytime after Phase 0
  |
  +---> Phase 8  (performance) -- can start anytime, but benefits from 2 & 3
```

### Parallelism opportunities

- **Phases 1, 4, 5, 6** can all begin concurrently once Phase 0 is done.
- **Phases 2 and 3** depend on Phase 1 only lightly (TierObserver fix).
  They can overlap with Phase 1 if 1.2 ships first.
- **Phase 7** benefits from Phases 2 and 3 being complete (more to test)
  but the harness (7.1) can be built in parallel.
- **Phase 8** is mostly independent and can interleave with anything.
- **Phase 9** is the long pole — it needs both encryption (4) and
  authorization (5) as foundations.

---

## Timeline Sketch (AI-swarm velocity)

| Milestone | Target | Bound by |
|-----------|--------|----------|
| Phase 0 complete | Week 1 | Code velocity |
| Phases 1–3 complete | Week 3 | Code velocity + integration test iteration |
| Phase 4 complete | Week 4 | Design review + KMS integration |
| Phase 5 complete | Week 5 | Design review |
| Phase 6 complete | Week 4 | Code velocity (parallel with 4–5) |
| Phase 7 complete | Week 6 | Bug discovery tail (unbounded part) |
| Phase 8 complete | Week 6 | Benchmark iteration |
| Phase 9 complete | Week 8 | Design complexity + depends on 4, 5 |
| Internal pilot ready | Month 3 | Real-hardware burn-in |
| Fortune 500 defensible | Month 12+ | SOC2 Type II, FIPS, reference customer |

---

## Deliverables for Future Planning Iterations

The next passes should split this scaffold into dedicated detailed plans.
*(Structure adopted from Codex.)*

1. **Runtime and transport integration plan** (Phase 0 + 0.6 harness)
2. **Security and encryption-at-rest plan** (Phases 1 security items + 4 + 5)
3. **Durability lifecycle plan** (Phase 2)
4. **Tiered storage closure plan** (Phase 3)
5. **Verification and fault-injection plan** (Phase 7)
6. **Resource-control and performance hardening plan** (Phase 8)
7. **Operations and pilot-readiness plan** (Phase 6 + runbooks + deployment docs)
8. **Multi-tenancy plan** (Phase 9)

Each follow-on plan should include:
- Exact problem statement
- Code ownership / subsystem boundaries
- Ordered implementation steps with file targets
- Test plan (unit, integration, and negative cases)
- Rollout and migration concerns
- Failure modes and rollback strategy

---

## Immediate Recommendation for Next Planning Pass

Start with a detailed **Phase 0 plan**: daemon runtime wiring, config
surface changes, QUIC/TLS bootstrap/join flow, real-network integration
harness, and minimal observability initialization needed to debug the
bring-up. *(Codex recommendation, adopted.)*

This is the narrowest high-leverage next step because it turns the rest
of the roadmap from library analysis into product-path execution. Every
other phase becomes testable against a real cluster the moment Phase 0
lands.

---

## Open Questions for Next Iteration

1. **Encryption ADR scope:** AES-256-GCM with random nonce per record, or
   XChaCha20-Poly1305? Performance implications for io_uring + O_DIRECT
   alignment need analysis. Gemini specifically called out the injection
   point as the `src/storage/core.rs` append path — confirm this is the
   right layer vs. encrypting at the slab-record serialization layer.
2. **Authorization model:** OID extension in X.509 certs vs. separate
   operator CA vs. token-based? Each has different operational ergonomics.
3. **Fault injection framework:** madsim vs. turmoil vs. custom
   deterministic simulator? madsim rewrites std/tokio; turmoil is lighter
   but less mature.
4. **Index persistence strategy:** Bolt-on persistent index (e.g. sled,
   redb) vs. periodic index checkpoint file vs. LSM from scratch?
5. **ObjectStore trait redesign:** Range-GET changes the trait shape.
   Should we also add streaming put, multipart, and list in one breaking
   change? All three reviewers flagged range-GET — do it early before more
   code accumulates on the current three-verb trait.
6. **Slab fsync group-commit vs. O_DIRECT guarantees:** Gemini flagged
   this as P0 under volatile write cache assumptions. Need to clarify
   target hardware (PLP NVMe assumed?) and whether O_DIRECT without
   explicit fsync is sufficient.
7. **Security floor bundling:** Codex argued for a single "security floor"
   phase combining encryption + authz + audit before any other work. Is
   the current split (Phases 4 + 5 in parallel) the right decomposition,
   or should we bundle them for enterprise gating speed?
