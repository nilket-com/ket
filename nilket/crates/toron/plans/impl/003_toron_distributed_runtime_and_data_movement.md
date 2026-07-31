# Implementation Plan: ADR 003 — Toron Distributed Runtime and Data Movement Contract

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion ADR:**
`plans/adr/003_toron_distributed_runtime_and_data_movement.md`
**Bench-results artifact (post-promotion):**
`plans/impl/003_distributed_runtime_bench_results.md`

## Inherits substrate

- `plans/adr/001_toron_foundation_and_engine_contract.md` —
  greenfield Arrow-native engine, single crate, `StorageScan`
  boundary trait, M0–M11 seed scaffold.
- `plans/adr/002_toron_storage_backed_execution_contract.md` —
  schema-bound `ExecutionPlan`, coordinator-owned binding,
  `Coordinator::admit_query` / `execute_query`, descriptor-
  fingerprint pin protocol, structured error taxonomy,
  `ScanStream` boundary, V1 aggregate contract, 7 architecture
  guards, canonical record-batch normalization (concat-and-
  reslice at 65,536 rows).
- `plans/impl/002_storage_execution_contract_bench_results.md`
  — bench evidence for ADR 002 promotion; ADR 003 reuses the
  Wendelon-native fixture corpus and extends with multi-
  process, kill-mid-query, and cross-replica determinism rows.
- `crates/wendelon/plans/adr/043_*.md` — wire / body
  compression (consumed indirectly via Arrow IPC).
- `crates/wendelon/plans/adr/061_*.md` — page-aligned
  authenticated chunk layout (encrypted spill backend composes
  with this primitive).
- M10 seed substrate — AES-256-GCM-SIV encrypted spill via
  `aws-lc-rs`, `wendelon::storage::file_ops::*`. Reused as the
  partition store's encrypted-spill backend.
- `crates/wendelon/plans/conventions/` — `newtype-units`,
  `no-version-suffixes`, `no-planning-phase-names`,
  `no-premature-backward-compat`, `no-project-name-in-
  protocols`, `capability-vs-feature-naming`.

## Status

Promoted to `plans/impl/`. Pre-implementation. Phase 0
calibration is the first phase.

## Implementation contract

- Coordinator binds `LogicalPlan` once per query (ADR 002),
  cuts the resulting `ExecutionPlan` into stage- and task-
  sized chunks, wraps each in `TaskSpec`.
- Workers receive `TaskSpec` over real process endpoints; no
  worker re-binds a `LogicalPlan`. Architecture-guarded.
- Every distributed wire message carries the versioned envelope
  `WireEnvelope { protocol_version, kind, identity, stream_seq,
  payload_len, trace, payload }`. Lifecycle messages carry
  `TaskIdentity`; data-movement messages carry
  `PartitionAddress`.
- Wire message kinds are the twelve in ADR §4 split across two
  surfaces: nine task-lifecycle (coordinator-observed FSM) +
  three data-movement (W↔W shuffle protocol). Seed
  `MorselComplete` / `Error(String)` retired.
- `TaskFailureKind` carries **7 variants**: `ProtocolMismatch`,
  `DescriptorEvolved`, `WorkerLost`, `TaskFailed`,
  `BudgetExhausted`, `Cancelled`, `Internal`. Six are
  worker-emittable on the wire; `WorkerLost` is
  coordinator-synthesized into the task FSM only (a lost
  worker cannot author `TaskFailed`). Architecture guard G15
  forbids `WorkerLost` from any encoded `TaskFailed` payload
  at the coordinator's wire ingress. Architecture-guarded
  against `Error(String)`.
- `TaskSpec` carries a `TaskIdentity` sub-struct with snake-
  cased `_id`-suffixed fields. Architecture-guarded that
  `TaskIdentity` is the only path to a task's distributed
  identity tuple.
- Partition-store wire has its own error surface:
  `PartitionMissing` is a *wire response* to `PullPartition`
  carrying `PartitionMissingReason` (`AttemptStale`,
  `CancelledTaskCleanup`, `WorkerLost`, `StoreEvicted`); stale-
  attempt detection surfaces as a coordinator-side
  `RuntimeError::PartitionStoreMismatch`, **not** a
  `TaskFailed` variant.
- Shuffle uses the pull-addressed partition store. Producer
  publishes `PartitionManifest`; consumer issues `PullPartition`
  requests. No P2P-push; no decoded-and-dropped shuffle path.
- Partition store has two backends: `InMemory` (V1 default) and
  `EncryptedSpill` (M10 substrate). Promotion thresholds:
  64 MiB unconsumed OR 5-second producer-to-consumer staleness
  window (Phase 0 P0.6 calibration). Promotion is one-way
  within a task.
- Coordinator maintains two layers (ADR §8):
  - **Per-attempt FSM** keyed by `(QueryId, StageId, TaskId,
    AttemptId)`: `Pending → Dispatched → Running →
    {Completed | FailedRetryable | FailedTerminal |
    Cancelled}`. Terminal states are terminal *for that
    attempt*.
  - **Per-task acceptance index** keyed by `(QueryId,
    StageId, TaskId)`: `Open → LiveAccepted(AttemptId) →
    {Invalidated(AttemptId) → Open | Failed | Cancelled}`.
    Producer-loss triggers `LiveAccepted` → `Invalidated`
    → `Open` without contradicting the per-attempt FSM's
    `Completed` terminal status.
  Accepted-attempt state is indexed by `(QueryId, StageId,
  TaskId)` — never bare `TaskId`, which is stage-local.
  Single retry default (`max_attempts = 2`) applies to the
  acceptance layer (number of spawned attempts per task);
  retry policy lives in `RetryPolicy`, not scattered
  constants.
- **Producer-loss invalidation.** When the registry marks a
  worker `Lost`, the coordinator walks the acceptance index
  for that `WorkerId`, retains the per-attempt FSM record
  `Completed(AttemptId)` for audit (that attempt did finish;
  the host died afterward), transitions the acceptance index
  `LiveAccepted(AttemptId) → Invalidated(AttemptId) → Open`
  for each affected producer task, spawns a fresh
  per-attempt FSM in `Pending` with a bumped `AttemptId`,
  and redispatches. Downstream consumers referencing
  invalidated manifests are cancelled (if unstarted) or fail
  with retryable `TaskFailed { reason: "producer manifest
  invalidated" }` (if mid-pull) and are redispatched after
  the producer reruns. The SIGKILL keystone fixture
  exercises this producer-loss case (the harder variant);
  consumer-loss is
  a separate easier fixture row.
- Per-query budget envelope (`QueryBudget`) allocated at
  admission; `BudgetExhausted` failure on cap hit.
- Static `WorkerRegistry` trait + `StaticWorkerRegistry`
  implementation; trait carries lease / capability / resource
  fields for future WeakControlRing swap.
- Strict stage barriers in V1: Stage N+1 dispatch occurs only
  after all Stage N manifests received and accepted.
- Location-agnostic fragment routing in V1.
- All 14 hard gates pass; all 8 architecture guards G8-G15
  pass; soft gates met or explicitly justified.

## Goal

Make Toron a real distributed compute ring. After ADR 003, a
multi-stage query (Scan + Filter + Aggregate + GROUP BY +
Shuffle + Final Aggregate) runs across multiple worker
processes, survives a worker SIGKILL mid-shuffle with byte-
identical normalized output, and bounds memory / shuffle /
spill through a per-query budget envelope without
`tokio::time::sleep` mocks.

## Phase plan

Each phase ships a runnable slice with fixture-backed tests.
Framework-only work does not count as complete (ADR 002
discipline, Wendelon ADR 070 precedent).

### Phase 0 — Audit, calibration, council pins

Pre-implementation pass. No code lands. Deliverable: a
"Phase 0 audit" section in the bench-results artifact pinning
each item.

- **P0.1: Multi-process test harness availability.** Audit
  the workspace for a pattern that spawns Toron worker
  processes from a test (likely via `std::process::Command`
  invoking the workspace binary with a `--worker` subcommand).
  Confirm `SIGKILL` is reachable via `nix::sys::signal::kill`
  or `libc::kill` on Linux. If a harness primitive doesn't
  exist, build it in Phase 1.

- **P0.2: Real-endpoint transport selection.** Toron's M5 seed
  used `tokio::io::duplex`; ADR 003 needs real process
  endpoints. The ADR contract is transport-agnostic.
  - **Preferred:** real QUIC via `quinn` if the dependency
    cost is acceptable and Toron can adopt it without crossing
    the `wendelon::network::{raft, quic}::*` import
    forbiddances.
  - **Acceptable:** framed-OS-socket through the existing
    `ToronTransport` trait (the M9 seed seam) if QUIC
    integration is expensive.

- **P0.3: M10 spill substrate audit.** Confirm
  `crates/toron/src/server/spill.rs` (seeded at M10) is
  reusable as the `EncryptedSpill` backend of the partition
  store: AES-256-GCM-SIV key generation per-spill-region,
  file-ops via `wendelon::storage::file_ops::AsyncFileOps`,
  cleanup on query termination, address-keyed lookup.

- **P0.4: Fixture matrix identification.** ADR 003 bench rows
  extend ADR 002's Wendelon-native fixture corpus with multi-
  worker shapes:
  - 2-worker / 4-worker / 8-worker scan + filter + aggregate.
  - GROUP BY with shuffle (2-stage DAG: partial-aggregate →
    shuffle → final-aggregate).
  - SIGKILL kill-mid-query (mid-shuffle, after manifest
    published).
  - Stale-attempt-rejection fixture (delayed-write race; a
    worker that was marked `Lost` returns and tries to publish
    completion).
  - Cancellation fixture (coordinator cancels mid-query;
    workers clean up reservations).
  - Backpressure-under-load fixture (forced budget exhaustion).
  - Cross-replica determinism (two independent runs, one with
    SIGKILL, both digest-equal after normalization).
  - Throughput-vs-worker-count rows on a GROUP BY workload at
    10M rows.

- **P0.5: Architecture-guard literal text.** Draft exact
  predicates for the eight new guards G8-G15:
  - **G8.** `WireEnvelope` is the only wire message wrapper;
    no message bypasses the envelope.
  - **G9.** Task lifecycle messages carry `TaskIdentity`
    (`(QueryId, StageId, TaskId, AttemptId)`); data-movement
    messages carry `PartitionAddress`. **Exception:**
    `Heartbeat` carries `(WorkerId, LeaseEpoch)` only — it
    is per-worker, not per-task, and predates task
    assignment. The guard is "every message carries
    routable identity"; for heartbeat that routable identity
    is `WorkerId`.
  - **G10.** No `tokio::time::sleep` in any
    `crates/toron/src/server/**/*.rs` or
    `crates/toron/src/distributed/**/*.rs` production path
    (test fixtures excepted).
  - **G11.** No production code initiates push-to-peer
    shuffle delivery; shuffle is `PullPartition`-only on the
    wire.
  - **G12.** No worker code path calls `Binder::bind`;
    binding is coordinator-only.
  - **G13.** Stage N+1 task dispatch in
    `Coordinator::dispatch_loop` is guarded by a
    `manifests_received_for_stage(N) == producer_count` check.
  - **G14.** `MorselId` does not appear in any distributed
    identity struct (`TaskIdentity`, `PartitionAddress`, wire
    message payloads); worker-local only.
  - **G15.** `MorselComplete` is not present in any
    `MessageKind` variant; the runtime lifecycle uses the
    twelve ADR 003 message kinds. `TaskFailureKind` does not
    contain `PartitionMissing` or `StaleAttempt` variants
    (compile-time + grep check); those live in
    `PartitionMissingReason` and `RuntimeError` respectively.
    `TaskFailureKind::WorkerLost` **must not appear** in any
    decoded `TaskFailed` wire payload at the coordinator's
    ingress (compile-time deny + ingress-side runtime check):
    `WorkerLost` is coordinator-synthesized into the FSM
    only — a worker that has lost contact cannot author a
    `TaskFailed` over the wire.

- **P0.6: Soft-gate threshold calibration.** Run microbenchmark
  estimates against a candidate fixture:
  - Partition-store memory→spill threshold (default 64 MiB).
  - Producer-to-consumer staleness window (default 5 seconds).
  - Heartbeat cadence (default 1 second; 3-miss timeout).
  - Worker-count scaling target (≥ 1.5× speedup per doubling).

- **P0.7: `WorkerRegistry` field set.** Pin concrete fields:
  - `WorkerEndpoint` — transport-endpoint address (host:port
    for QUIC; OS socket path for framed-socket transport).
  - `WorkerCapabilities` — advertised memory cap, SIMD width,
    spill capacity reserved.
  - `WorkerLease { last_heartbeat, epoch }` — epoch bumps on
    `mark_lost`.
  - `WorkerResourceState` — coarse live load (in-flight bytes,
    active tasks).
  - `WorkerStatus { Live | Suspect | Lost }` — `Suspect` after
    first missed heartbeat, `Lost` after threshold.

- **P0.8: `chore:` cleanup commits.** Roadmap status updates,
  comment cleanup, test relocation per ADR 002 P0.8 pattern.

- **P0.9: Canonical-normalization helper extension.** The
  shared helper `canonical_normalize(batches, sort_policy)`
  gains a `SortPolicy` parameter, and the operation order
  is pinned to avoid sort-after-reslice batch-boundary
  ambiguity:
  - `SortPolicy::None` — ADR 002 callsites; pipeline is
    **concat → reslice (65,536-row batches) → digest**.
    Batch-boundary canonicalization only; no row-order
    canonicalization.
  - `SortPolicy::FullRowLex` — ADR 003 distributed callsites;
    pipeline is **concat → stable lex full-row sort (all
    columns left-to-right, deterministic null ordering) →
    reslice (65,536-row batches) → digest**.
  The sort runs **before** reslice. Resliced-then-sorted
  would only canonicalize row order within each 65,536-row
  block, leaving cross-block order non-deterministic — that
  would defeat the gate. Concat → sort → reslice is the
  byte-stable pipeline per ADR §13. Concat-and-reslice
  without the sort is insufficient under distributed
  hash-aggregation + variable worker assignment + map
  iteration nondeterminism; the full-row sort is the
  weakest normalization that survives those without masking
  real correctness bugs (value drift, lost rows, duplicated
  rows). Phase 0 pins the exact comparison helper used by
  every ADR 003 digest fixture.

### Phase 1 — Identity newtypes, `TaskSpec`, runtime errors

Land first because every later phase depends on them.

Files:
- `src/distributed/identity.rs` (new) — distributed newtypes
  and `TaskIdentity`.
- `src/distributed/task.rs` (new) — `TaskSpec`,
  `TaskInputBinding` (carries materialized
  `Vec<ProducerPartitionRef>` per ADR §1, populated at
  consumer-task dispatch time from accepted producer
  manifests), `TaskOutputBinding`, `TaskBudget`,
  `QueryBudget`, `RetryPolicy`, `ProducerPartitionRef`.
- `src/error/distributed.rs` (new) — `TaskFailureKind` (7
  variants), `BudgetDimension`, `PartitionMissingReason` (wire
  enum, 4 variants), `RuntimeError::PartitionStoreMismatch`,
  `RuntimeError::ProtocolMismatch`.
- `src/ir/execution.rs` (extend if needed) — exchange / stage
  metadata helpers.

Work:
- **`QueryId` migration.** Seed code has `QueryId(pub u128)`
  in `src/ir/execution.rs:22`. ADR 003 codifies
  `QueryId(Uuid)` to match the distributed identity catalog
  and the wire-stable representation. Phase 1 migrates the
  existing definition in place (single canonical home in
  `src/distributed/identity.rs`; `ir/execution.rs` and `ir.rs`
  re-export). Construction sites in the coordinator,
  catalog, and tests use `QueryId::new()` →
  `Uuid::now_v7()`. **UUIDv7 over v4:** v7 prefixes 48 bits
  of Unix-millisecond timestamp before the random suffix, so
  even under degraded entropy the per-ms namespace shrinks
  practical collision probability to negligible, and the
  time prefix makes `QueryId`s sort by creation time —
  useful for log scans, accepted-attempt index iteration,
  and bench replay. Same `uuid` crate, just the `v7`
  feature instead of `v4`. No backwards-compat shim — ADR
  002 was in-process duplex only and has no persisted
  `QueryId` representation to preserve.
- **`StageId` reused.** Seed code has `StageId(pub u32)` in
  `src/ir.rs:14`. ADR 003 keeps the same shape. Move the
  canonical definition into `src/distributed/identity.rs`
  alongside the rest of the identity catalog and re-export
  from `src/ir.rs` for backwards compatibility within the
  crate.
- Add the 8 new newtypes: `TaskId`, `AttemptId`, `WorkerId`,
  `PartitionId`, `ExchangeId`, `LeaseEpoch`, `FrameByteLen`,
  `StreamSeq`. `WorkerId` is a `Uuid` constructed via
  `Uuid::now_v7()` (same v7 rationale as `QueryId`); `u32`
  for the rest except `StreamSeq` (`u64`).
- Add `TaskIdentity { query_id, stage_id, task_id,
  attempt_id }`.
- Add `TaskSpec`, `TaskInputBinding`, `TaskOutputBinding`,
  `TaskBudget`, `QueryBudget`, `RetryPolicy`.
- Define `TaskFailureKind` with seven variants. Compile-time
  guard: no `PartitionMissing` / `StaleAttempt` variants.
- Define `PartitionMissingReason` (wire enum).
- Integrate `RuntimeError` into `QueryError` without
  stringifying remote failures.
- Architecture guard G14: `MorselId` is worker-local only.

Tests:
- Newtype construction and equality.
- `TaskIdentity` serialization round-trip.
- `MorselId` absence from distributed identity structs.
- `RetryPolicy` default is one retry per task.
- Runtime errors preserve typed variants through `QueryError`.
- `TaskFailureKind` has exactly 7 variants (compile-time +
  grep guard).
- `QueryId` migrated from `u128` to `Uuid` at one canonical
  home; legacy callsites updated.
- Round-trip `WorkerLost` through the coordinator FSM (it
  must not appear on a decoded wire `TaskFailed` payload —
  G15 enforces).

### Phase 2 — Wire envelope and lifecycle messages

Files:
- `src/wire/envelope.rs` (new or split from `src/wire.rs`) —
  `WireEnvelope`, `ProtocolVersion`, `MessageKind`,
  `MessagePayload`.
- `src/wire/messages.rs` (new) — 12 lifecycle / data-movement
  message payloads.
- `tests/architecture_test.rs` (extend) — G8, G9, G15.

Work:
- Replace `ToronMessage` as the distributed contract with
  `WireEnvelope` + `MessageKind` + payload structs.
- **Task lifecycle (9, coordinator-observed FSM):**
  - `StartTask { spec: TaskSpec }` — `spec.identity` carries
    the full `TaskIdentity`.
  - `TaskStarted { identity: TaskIdentity }`
  - `TaskBatch { identity, stream_seq: StreamSeq,
    payload: ArrowIpcBytes }`
  - `TaskComplete { identity, output: TaskCompletionOutput }`
  - `TaskFailed { identity, kind: TaskFailureKind }`
  - `CancelTask { identity }`
  - `TaskCancelled { identity }`
  - `Progress { identity, bytes_produced, rows_produced }`
  - `Heartbeat { worker_id: WorkerId, lease_epoch: LeaseEpoch }`
- **Data movement (3, W↔W shuffle protocol, addressed by
  `PartitionAddress`, not part of the task FSM):**
  - `PullPartition { address: PartitionAddress,
    expected_attempt: AttemptId }`
  - `PartitionData { address, stream_seq, payload }`
  - `PartitionMissing { address, reason: PartitionMissingReason }`
- Add `protocol_version: ProtocolVersion` as an envelope field.
- Add max-frame validation using `FrameByteLen` against
  `QueryBudget::max_arrow_ipc_frame_bytes` before payload
  allocation.
- Reject incompatible protocol versions with
  `RuntimeError::ProtocolMismatch`.
- Retire seed `ToronMessage::{ArrowBatch, ShuffleData,
  MorselComplete, Error(String)}`.

Tests:
- Envelope encode/decode round-trip for every `MessageKind`
  variant.
- Protocol-version mismatch fails with typed `RuntimeError`.
- Oversized frame fails before payload allocation.
- `PartitionMissing` round-trips with all four
  `PartitionMissingReason` variants.
- Architecture guard G8 (envelope-only).
- Architecture guard G9 (identity-tuple presence).
- Architecture guard G15 (`MorselComplete` absent).

### Phase 3 — Pull-addressed partition store

Files:
- `src/distributed/partition_store/` (new module):
  - `mod.rs`: `PartitionStore` trait, `PartitionAddress`,
    `PartitionManifest`, `PartitionEntry`, `PartitionBackend`.
  - `in_memory.rs`: `InMemoryPartitionStore` with budget
    tracking.
  - `encrypted_spill.rs`: `EncryptedSpillPartitionStore`
    wrapping the seed M10 `SpillManager`.
  - `hybrid.rs`: `HybridPartitionStore` — composes in-memory
    (default) with encrypted-spill (promotion on threshold).
- `src/server/worker.rs` (extend) — `PullPartition` request
  handler.

Work:
- Implement the three partition-store backends.
- One-way backend promotion: once a task spills, it stays
  spilled.
- Producer worker accepts `PullPartition { address,
  expected_attempt }`; rejects with `PartitionMissing
  { reason: AttemptStale }` if `expected_attempt` doesn't
  match the local manifest's accepted attempt.
- Producer worker emits `TaskComplete { output:
  ShuffleProducerOutput(manifest) }` on stage completion;
  the manifest carries `producer_worker_id: WorkerId` and
  `producer_endpoint: WorkerEndpoint` (per ADR §5) so the
  coordinator can materialize `ProducerPartitionRef` into
  downstream consumer `TaskSpec.inputs` without consulting
  the registry at every dispatch.
- Coordinator records the accepted manifest in its task
  state, indexed by `(QueryId, StageId, TaskId)`. The
  coordinator tracks, per accepted manifest, the set of
  consumer attempts that referenced its partitions and
  promotes the producer task to `Drained` only when **every
  such consumer has itself reached `LiveAccepted` in the
  acceptance index** — not merely "every consumer finished
  pulling." Pull completion is too early: a consumer can
  finish pulling and then fail before its own `TaskComplete`
  is accepted, and its retry would still need producer
  data. `Drained` propagates up the DAG: a producer is
  `Drained` only when its consumers are themselves
  `Drained`-or-final-`LiveAccepted` (per ADR §8).
- **Producer-loss invalidation handler** in the coordinator:
  on `WorkerRegistry` `Live → Lost` transition, walk the
  acceptance index for that `WorkerId`. For each
  `LiveAccepted(AttemptId)` entry, **skip if the task is
  already `Drained`** (every downstream consumer attempt
  has itself reached `LiveAccepted` transitively up to the
  query result — partition state being gone is harmless,
  rerunning would be wasted work and would invalidate
  consumer attempts that already legitimately observed the
  output). Otherwise, transition the affected entry
  `LiveAccepted(AttemptId) → Invalidated(AttemptId)` (the
  per-attempt FSM's `Completed(AttemptId)` record is
  retained for audit — that attempt *did* finish, the
  partitions just died after acceptance; this is the
  two-layer split from ADR §8). The task acceptance index
  then transitions `Invalidated(AttemptId) → Open` and
  spawns a fresh per-attempt FSM in `Pending` with a
  bumped `AttemptId`. Cascade cancellation / redispatch to
  downstream consumers per ADR §8.
- Architecture guard G11: no P2P-push patterns appear in
  `src/server/**`.

Tests:
- In-memory roundtrip: publish 100 partitions, pull, byte-
  identical.
- Encrypted-spill roundtrip: same.
- Promotion threshold: publish 100 MiB across 4 partitions
  with 64 MiB threshold; observe in-memory for the first
  batch, spill for subsequent; verify no demotion.
- Stale-attempt rejection: pull with stale `expected_attempt`;
  producer responds with `PartitionMissing { reason:
  AttemptStale, expected_attempt, observed_attempt }`.
  Consumer pull handler surfaces
  `RuntimeError::PartitionStoreMismatch` coordinator-side;
  consumer task either resyncs (if coordinator's accepted
  attempt matches producer's current) or fails with
  `TaskFailureKind::TaskFailed { retryable: true,
  reason: "stage barrier resync" }`. **Not**
  `TaskFailureKind::PartitionMissing` — that variant does not
  exist.
- Pull from missing partition returns typed `PartitionMissing`.
- **Producer-loss invalidation — pre-pull case**: mark a
  producer worker `Lost` after its manifest has been
  accepted but before any downstream consumer has pulled.
  Verify the coordinator invalidates the manifest, retains
  the per-attempt FSM `Completed(AttemptId)` record (the
  attempt did complete — the host died after), transitions
  the acceptance index `LiveAccepted(AttemptId) →
  Invalidated(AttemptId) → Open`, spawns a fresh per-attempt
  FSM with a bumped `AttemptId`, redispatches to a live
  worker, and propagates the cancellation / redispatch to
  downstream consumers per ADR §8.
- **Producer-loss invalidation — post-pull pre-acceptance
  case** (this is the case the iter-7 review flagged):
  consumer C finishes pulling from producer P (host A),
  acknowledges pull. Then C fails before its own
  `TaskComplete` is accepted (e.g., simulated OOM in the
  final aggregate). Then worker A is killed. C is not yet
  `LiveAccepted`, so P is not yet `Drained`. Verify the
  coordinator **does** invalidate P's manifest, reruns P,
  then reruns C against the new producer attempt — both
  succeed, final digest equals baseline. Pull completion
  alone must not promote a producer to `Drained`.
- **Producer-loss invalidation skip (`Drained` case)**: mark
  a producer worker `Lost` **after** every downstream
  consumer attempt that pulled has itself reached
  `LiveAccepted` (the consumer's own `TaskComplete` has been
  accepted, transitively up to the query result, so the
  producer is `Drained`). Verify the coordinator does
  **not** invalidate the manifest, does **not** rerun the
  producer task, and does **not** disturb the consumer
  attempts that already legitimately observed the output.
  The host going away is benign once every consumer that
  needed the partitions has itself been accepted.
- Architecture guard G11 fires on any push-to-peer pattern.

### Phase 4 — Worker registry

Files:
- `src/distributed/worker_registry/` (new module):
  - `mod.rs`: `WorkerRegistry` trait, `WorkerEntry`,
    `WorkerEndpoint`, `WorkerCapabilities`,
    `WorkerResourceState`, `WorkerLease`, `WorkerStatus`.
  - `static_registry.rs`: `StaticWorkerRegistry` — hand-
    configured list.

Work:
- Implement the trait surface: `list`, `resolve`,
  `select_for_attempt`, `mark_lost`, `heartbeat`.
- Heartbeat handling in the coordinator: receive
  `Heartbeat { worker_id, lease_epoch }`; update
  `WorkerLease::last_heartbeat`. Background task scans for
  `last_heartbeat` aged beyond threshold and transitions
  `Live → Suspect → Lost`.

Tests:
- Static registry returns hand-configured list.
- Heartbeat updates lease.
- Missed heartbeats → `Suspect` → `Lost` transitions.
- `Lost` worker is not selected for new attempts.

### Phase 5 — Coordinator scheduler + task FSM

Files:
- `src/coordinator.rs` (extend or split into `src/coordinator/`).
- `src/planner.rs` (if stage cutting lives here rather than in
  the coordinator).

Work:
- `Coordinator::admit_query` (existing from ADR 002) — no
  change; bind + plan + pin fingerprints.
- **New** `Coordinator::fragment_plan` — cut `ExecutionPlan`
  into stages + tasks:
  - Walk `ExecutionPlan` bottom-up; insert `StageId`
    boundaries at shuffle / final-result junctures.
  - Within each stage, create one `TaskId` per partition. For
    source stages (Scan), one task per `Scan` source split.
    For shuffle-consumer stages, one task per `PartitionId`.
  - Allocate `ExchangeId` per shuffle boundary.
  - Build `TaskSpec` for each task with `TaskIdentity`.
- **Consumer-task input materialization.** When the
  coordinator dispatches a consumer-stage task whose
  `TaskInputBinding == ShuffleConsumer { ... }`, it
  materializes the `producer_partitions:
  Vec<ProducerPartitionRef>` from the accepted upstream
  manifests at dispatch time. Each ref carries the
  `PartitionAddress`, `WorkerId`, and `WorkerEndpoint` the
  worker needs to issue `PullPartition` directly — no
  registry round-trip from the worker. Materialization
  happens after the strict stage-barrier check fires
  (Stage N manifests fully accepted), so worker locations
  are stable for the dispatch.
- **New** `Coordinator::dispatch_loop` — drive both layers:
  - Pick tasks whose acceptance index is `Open` and whose
    upstream-stage acceptance is uniformly
    `LiveAccepted(...)` (strict barrier).
  - Spawn a new per-attempt FSM in `Pending`; assign
    `TaskId → WorkerId` via
    `WorkerRegistry::select_for_attempt`.
  - Send `StartTask { spec }` over the wire.
  - Track per-attempt `Dispatched → Running →
    {Completed | FailedRetryable | FailedTerminal |
    Cancelled}`.
  - On per-attempt `Completed`: task acceptance index
    transitions `Open → LiveAccepted(AttemptId)`.
  - On per-attempt `FailedRetryable`: leave acceptance index
    `Open`; if `max_attempts` not yet exhausted, spawn the
    next per-attempt FSM with a bumped `AttemptId`.
  - On per-attempt `FailedTerminal` or `max_attempts` hit:
    transition acceptance index to `Failed`; fail query with
    `QueryError`.
  - On `Live → Lost` transition for a worker hosting a
    `LiveAccepted` attempt: transition
    `LiveAccepted(AttemptId) → Invalidated(AttemptId) →
    Open` and spawn a fresh attempt (producer-loss path).
- Stage-barrier enforcement: `dispatch_loop` does not dispatch
  Stage N+1 tasks until every Stage N task's acceptance
  index is `LiveAccepted(...)` (not just per-attempt
  `Completed`). Reading the acceptance layer instead of the
  per-attempt FSM is what makes producer-loss invalidation
  safe — if a Stage N task drops back to `Open`, Stage N+1
  dispatch pauses for that task again.
- Architecture guard G13.

Tests:
- Single-stage query: dispatch, complete, no shuffle.
- 2-stage query: stage barrier enforced.
- Task failure: retry once, then fail terminal.
- `max_attempts = 2` honored.
- Stage-barrier guard fires on any out-of-order dispatch
  attempt.
- Stale `TaskComplete` rejected.
- Stale `TaskBatch` rejected.

### Phase 6 — Worker fragment executor

Files:
- `src/server/worker.rs` (rewrite) — consume `TaskSpec`.
- `src/server/mod.rs` (extend).
- `src/exec.rs` (extend if executor needs a narrower entry
  point for bound fragments).

Work:
- Rewrite worker to consume `TaskSpec` over the wire:
  - Receive `StartTask { spec }`.
  - Emit `TaskStarted { identity }`.
  - Execute `spec.plan` via the ADR 002 `Executor` (no
    re-binding; `ExecutionPlan` is already bound).
  - Stream Arrow IPC batches via `TaskBatch`.
  - On completion:
    - `FinalResult`: emit `TaskComplete { output:
      FinalResultOutput }`.
    - `ShuffleProducer { exchange_id, partition_count,
      partition_key }`: partition the output via the partition
      key, publish to local partition store, emit
      `TaskComplete { output:
      ShuffleProducerOutput(manifest) }`.
  - For shuffle-consumer tasks: open `PullPartition` requests
    against the producers named in `spec.inputs`, stream
    batches into the local executor.
- Worker-local morsel splitting (V1 unchanged from seed M4
  shape; morsels are local scheduling).
- Architecture guard G12: no worker code path calls
  `Binder::bind`.

Tests:
- Scan + Filter + Aggregate in one worker, no shuffle: output
  matches in-memory baseline.
- Shuffle producer: partition manifest published with correct
  entries.
- Shuffle consumer: pulls from producer, executes final
  aggregate, output matches baseline.
- Worker rejects `LogicalPlan` dispatch (legacy
  `ExecuteMorsel` path errors with
  `TaskFailureKind::ProtocolMismatch`).

### Phase 7 — Per-query budget envelope

Files:
- `src/distributed/budget/` (new module):
  - `QueryBudget` struct.
  - `BudgetReservation` RAII guard.
  - `BudgetTracker` per-query, per-dimension counters.
- `src/server/memory.rs` (extend) — budget-aware reservation
  hooks.

Work:
- Coordinator-side allocation in `Coordinator::admit_query`:
  allocate `QueryBudget` from a per-cluster default (Phase 0
  P0.6 calibrated values).
- Worker-side enforcement: each task acquires its share of
  `memory_per_task_bytes` / `shuffle_bytes_published_per_task`;
  emits `TaskFailed { kind: BudgetExhausted, ... }` on
  reservation failure.
- Coordinator decides retryability based on
  `BudgetDimension`:
  - `Memory` / `ShuffleBytes` → retryable.
  - `SpillBytes` / `BytesInFlightPerQuery` / `FrameSize` →
    terminal (per ADR §6; oversize frames are a producer-
    authoring problem that won't resolve on a different
    worker against the same per-query cap).
- No `tokio::time::sleep` for backpressure. Architecture guard
  G10.

Tests:
- Memory exhaustion → `BudgetExhausted { dimension: Memory }`
  → retry on different worker.
- Spill exhaustion → `BudgetExhausted { dimension:
  SpillBytes }` → terminal failure.
- Frame oversize → `BudgetExhausted { dimension: FrameSize }`
  → terminal failure (not retried).
- Architecture guard G10 fires on any new
  `tokio::time::sleep` in `src/server/**` or
  `src/distributed/**`.

### Phase 8 — Cancellation propagation

Files:
- `src/coordinator.rs` (extend).
- `src/server/worker.rs` (extend).

Work:
- `CancelTask → TaskCancelled` lifecycle: coordinator emits
  `CancelTask` to every worker hosting an in-flight task for
  a cancelled query; workers stop execution, release budget
  reservations, emit `TaskCancelled`.
- Cancellation grace window (Phase 0 P0.6: default 1 second).
  Workers that don't respond within the window are marked
  `Suspect` → redispatched-or-fail-query.
- Stale-output suppression: output arriving from a cancelled
  task's `AttemptId` is rejected by the coordinator.
- Cleanup of unaccepted partition-store entries on
  cancellation.

Tests:
- Query cancel mid-execution: all workers cancel cleanly
  within grace window.
- Cancelled task's late `TaskBatch` is dropped (not accepted).
- Post-cancel partition publication is rejected.
- Budget reservations released on cancellation.
- Heartbeat updates worker registry state during cancellation
  cleanup.

### Phase 9 — Real-endpoint integration, SIGKILL keystone, cross-replica determinism

Files:
- `src/server/transport.rs` (rewrite or extend) — real-
  endpoint transport (QUIC via `quinn` if Phase 0 selects it,
  else framed OS sockets through `ToronTransport`).
- `tests/distributed_runtime_test.rs` (new) — multi-process
  fixtures.
- `tests/shuffle_runtime_test.rs` (new) — partition store,
  manifest, stale-attempt fixtures.

Work:
- Real-endpoint transport implementation through
  `ToronTransport`.
- Multi-process test harness: spawn N worker processes via
  `std::process::Command` invoking the workspace binary with
  a `--worker` subcommand; configure coordinator with the
  worker `WorkerEntry` list; run query; optionally `SIGKILL`
  a worker mid-execution; compare output to baseline.
- **SIGKILL test (hard gate #4) — producer-loss variant:**
  run a 2-stage GROUP BY query over a 10M-row Wendelon-
  native fixture across 4 workers; mid-shuffle (after at
  least one producer manifest is **accepted by the
  coordinator**, before all consumers complete), `SIGKILL`
  a worker hosting an accepted producer manifest (the
  harder case where producer-hosted partition state dies
  with the worker); coordinator detects via QUIC drop +
  heartbeat timeout (3 missed at 1-second cadence); applies
  the producer-loss invalidation rule (§8): the per-attempt
  FSM record `Completed(AttemptId)` is retained for audit
  (that attempt did finish — the host died afterward), the
  acceptance index transitions `LiveAccepted(AttemptId) →
  Invalidated(AttemptId) → Open`, a fresh per-attempt FSM
  spawns in `Pending` with a bumped `AttemptId`, dispatched
  to a live worker; in-flight consumer tasks referencing
  the invalidated manifest fail with `TaskFailed
  { retryable: true, reason: "producer manifest
  invalidated" }` and are redispatched after the new
  producer attempt completes; query completes; normalized
  output digest (concat → full-row sort → reslice → digest
  per §13) equals no-failure baseline.
- **Consumer-kill variant** (separate, easier fixture row):
  SIGKILL a worker hosting only consumer tasks; verify
  redispatch without invalidating any producer manifest.
- **Stale-attempt-rejection test (hard gate #5):** a delayed-
  write race fixture where a worker that was marked `Lost`
  returns and tries to publish `TaskComplete` for its old
  `AttemptId`; coordinator rejects with stale-attempt
  diagnostic.
- **Cross-replica determinism test (hard gate #11):** two
  independent runs of the same query (one with SIGKILL); both
  produce byte-identical Arrow IPC output after the ADR §13
  canonical normalization pipeline (in this exact order):
  **concat all output batches → stable lexicographic sort
  over the full row (all columns left-to-right,
  deterministic null ordering) → reslice to 65,536-row
  batches → digest**. The sort runs *before* reslice;
  reslicing first would only canonicalize row order within
  each 65,536-row block, leaving cross-block order non-
  deterministic. ADR 002's concat-then-reslice-only path
  is insufficient under distributed hash-aggregation +
  variable worker assignment + map iteration nondeterminism,
  the full-row sort is required (ADR §13). The shared
  helper `canonical_normalize(batches, SortPolicy::FullRowLex)`
  is the only digest-comparison path for ADR 003 parity
  tests.
- **`ExchangeId` manifest coverage test (hard gate #13):**
  every `PartitionAddress` carries the `ExchangeId` of its
  source shuffle boundary; every `PartitionManifest` includes
  the matching `ExchangeId`; mismatches between
  `TaskInputBinding::ShuffleConsumer { exchange_id }` and the
  producer manifest fail with `PartitionMissing`.
- **Registry availability test (hard gate #14):** fixture
  marks a worker `Lost` mid-query; coordinator routes the
  next `AttemptId` to a different worker; the `Lost` worker
  is not selected for any subsequent attempt within the
  query.

Tests:
- All hard gates 1-14 covered.
- Two-worker no-failure result digest.
- Worker-failure redispatch result digest equals baseline.
- SIGKILL mid-shuffle result digest equals baseline.
- Repeated multi-worker run digest equals baseline.
- Consumer stage starts only after producer manifests are
  accepted.

### Phase 10 — Bench matrix and promotion

Records all promotion gates from the ADR. Lives in
`plans/impl/003_distributed_runtime_bench_results.md`.

Files:
- `benches/distributed_runtime.rs` (new) — mirrors Wendelon's
  per-ADR bench-results pattern.

Work:
- Bench flag: `--distributed-runtime-only`.
- Required query shapes:
  - 2-stage GROUP BY (partial-agg → shuffle → final-agg) at
    2 / 4 / 8 workers.
  - Scan + Filter + final-only aggregate (no shuffle) at
    2 / 4 / 8 workers.
  - SIGKILL keystone (one run per worker count).
  - Stale-attempt-rejection (deterministic fixture).
  - Cancellation propagation timing.
  - Budget exhaustion (memory + spill dimensions).
  - Cross-replica determinism (digest pair per fixture).
- Per-row metrics:
  - worker count
  - stage count
  - task count
  - exchange count
  - shuffle bytes published
  - shuffle bytes pulled
  - retry count
  - stale-output rejection count
  - cancellation cleanup count
  - result digest
  - wall-clock baseline (single-worker) vs distributed
  - per-worker memory peak
  - SIGKILL detection latency

Verification commands:

```text
# Local quick verification
cargo fmt
cargo check --workspace
cargo test -p toron
cargo test -p toron --test architecture_test
cargo test -q distributed::partition_store
cargo test -q distributed::worker_registry
cargo test -q distributed::budget
cargo test -q coordinator::dispatch_loop
cargo test -p toron --test distributed_runtime_test
cargo test -p toron --test shuffle_runtime_test

# Pre-promotion full verification
cargo test --workspace
cargo bench --bench distributed_runtime --no-run
cargo bench --bench distributed_runtime -- --quick --distributed-runtime-only
cargo bench --bench distributed_runtime -- --distributed-runtime-only --workers=2,4,8
cargo bench --bench distributed_runtime -- --distributed-runtime-only --kill-test
git diff --check
```

Suggested commit slices:
1. `chore: adr 003 prep — roadmap honesty + multi-process harness scaffold`
2. `plan: adr 003 toron distributed runtime`
3. `feat: adr 003 phase 1 — identity newtypes + task spec + runtime errors`
4. `feat: adr 003 phase 2 — wire envelope + message lifecycle`
5. `feat: adr 003 phase 3 — pull-addressed partition store`
6. `feat: adr 003 phase 4 — worker registry`
7. `feat: adr 003 phase 5 — coordinator scheduler + task FSM`
8. `feat: adr 003 phase 6 — worker fragment executor`
9. `feat: adr 003 phase 7 — per-query budget envelope`
10. `feat: adr 003 phase 8 — cancellation propagation`
11. `feat: adr 003 phase 9 — real endpoints + SIGKILL keystone + cross-replica determinism`
12. `feat: adr 003 phase 10 — bench matrix + promotion` (lands
    `bench_results.md`; marks ADR 003 shipped on roadmap)

Roadmap update: `plans/wip/roadmap.md` records ADR 003
promotion; forward sequence updated to reflect ADR 004
candidates (distributed joins, multi-tenant governance,
executor model, Rune sandbox enforcement) — evidence-driven
from this bench matrix.

If hard gates pass, promote. If a hard gate fails, the council
decides among three explicit paths (ADR 070 precedent):

1. **Revise V1:** narrow scope (e.g., drop one shuffle
   backend, defer cancellation correctness), tighten oracle.
2. **Scope down:** ship transport + lifecycle + identity as
   plumbing; defer partition store or budget envelope to
   ADR 003.x.
3. **Shelve:** keep the seed unchanged, document the gap,
   reopen when fixture matrix tells us where to look.

## Files touched

Implementation:

- `crates/toron/src/distributed/identity.rs` (new) — 10
  distributed newtypes + `TaskIdentity`.
- `crates/toron/src/distributed/task.rs` (new) — `TaskSpec`,
  `TaskInputBinding`, `TaskOutputBinding`, `TaskBudget`,
  `QueryBudget`, `RetryPolicy`.
- `crates/toron/src/distributed/partition_store/` (new module)
  — `mod.rs`, `in_memory.rs`, `encrypted_spill.rs`,
  `hybrid.rs`.
- `crates/toron/src/distributed/worker_registry/` (new module)
  — `mod.rs`, `static_registry.rs`.
- `crates/toron/src/distributed/budget/` (new module) —
  `QueryBudget`, `BudgetReservation`, `BudgetTracker`.
- `crates/toron/src/wire/envelope.rs` (new) — `WireEnvelope`,
  `ProtocolVersion`, `MessageKind`, `MessagePayload`.
- `crates/toron/src/wire/messages.rs` (new) — lifecycle and
  data-movement message payloads.
- `crates/toron/src/error.rs` (extend) — wire in
  `RuntimeError`, `TaskFailureKind`, `PartitionMissingReason`.
- `crates/toron/src/error/distributed.rs` (new) — distributed
  error types.
- `crates/toron/src/coordinator.rs` (extend or split into
  `coordinator/`) — `fragment_plan`, `dispatch_loop`, task
  FSM, stage-barrier enforcement, cancellation propagation,
  budget allocation.
- `crates/toron/src/server/worker.rs` (rewrite) — consumes
  `TaskSpec`, no re-binding, partition store integration,
  `PullPartition` handler.
- `crates/toron/src/server/transport.rs` (rewrite) — real-
  endpoint transport (QUIC via `quinn` if Phase 0 selects it,
  else framed OS sockets).
- `crates/toron/src/server/spill.rs` (extend) — partition-
  store spill backend integration.
- `crates/toron/src/server/memory.rs` (extend) — budget-aware
  reservation hooks.
- `crates/toron/src/planner.rs` (extend if stage cutting lives
  here).
- `crates/toron/src/exec.rs` (extend if executor needs a
  narrower entry point for bound fragments).
- `crates/toron/tests/architecture_test.rs` (extend) — guards
  G8 through G15.
- `crates/toron/tests/distributed_runtime_test.rs` (new) —
  multi-process fixtures, SIGKILL keystone, cross-replica
  determinism, cancellation.
- `crates/toron/tests/shuffle_runtime_test.rs` (new) —
  partition store, manifest, stale-attempt fixtures.
- `crates/toron/benches/distributed_runtime.rs` (new) — bench
  matrix.
- `crates/toron/src/lib.rs` — wire up new modules.
- `crates/toron/Cargo.toml` — add `quinn` (if Phase 0 chooses
  it), `nix` (for SIGKILL in tests), `uuid` with the `v7`
  feature (for `QueryId` / `WorkerId`).

Retired:

- Seed `ToronMessage::{ArrowBatch, ShuffleData, MorselComplete,
  Error(String)}` variants.
- Seed sleep-based backpressure throttle.
- Seed decoded-and-dropped shuffle path.

Planning / evidence:

- `plans/adr/003_toron_distributed_runtime_and_data_movement.md`
  (this ADR pair).
- `plans/impl/003_toron_distributed_runtime_and_data_movement.md`
  (this file).
- `plans/impl/003_distributed_runtime_bench_results.md` (after
  Phase 10 evidence exists).

Roadmap:

- `plans/wip/roadmap.md` — record ADR 003 promotion; reframe
  ADR 004 candidates as evidence-driven from bench results.

Out of scope:

- Distributed joins. ADR 004+.
- Multi-tenant governance. ADR 005+.
- Pipelined cross-stage execution. V1.x.
- Locality-aware fragment routing. V1.x.
- WeakControlRing implementation. V1.x.
- RDMA / RoCEv2 / SIW. V1.x.
- Operator-internal streaming end-to-end. V1.x executor work.
- Replicated / disaggregated shuffle service. V2.
- Direct P2P-push fast path. V2 optimization.

## Risks and mitigations

- **R1: Real QUIC primitive availability.** If
  `wendelon::network` doesn't expose a clean non-Wendelon
  consumer surface and `quinn` brings dependency weight.
  *Mitigation:* Phase 0 P0.2 audit; if `quinn` is the answer,
  accept the dep — it's the ecosystem standard. Framed OS
  sockets through `ToronTransport` is the fallback. Wendelon-
  side extraction of a `wendelon-net` shared crate is V1.x
  territory regardless.
- **R2: SIGKILL test flakiness.** Real process kill +
  heartbeat-timeout detection has inherent timing sensitivity.
  *Mitigation:* tune timeouts generously for the test
  (10× expected detection latency); rely on transport stream
  drop as the primary signal (fast); heartbeat timeout as
  backup (slower, more deterministic).
- **R3: Partition-store promotion threshold thrash.** If the
  64 MiB threshold causes pathological flip-flopping.
  *Mitigation:* one-way promotion within a task (once
  spilled, stay spilled); Phase 0 P0.6 calibration.
- **R4: Cross-replica determinism noise from worker-
  assignment variance.** ADR 003 commits to output parity,
  not assignment parity, but the digest comparison is
  sensitive to any internal nondeterminism (hash seed,
  iteration order).
  *Mitigation:* the ADR 003 canonical normalization (§13)
  = concat-and-reslice (ADR 002 §"Resolved Q4", fixes
  batch boundaries) **plus** stable full-row lexicographic
  sort (fixes row order under hash-aggregation /
  partition-arrival / map-iteration nondeterminism). ADR
  002's concat-only normalization is **not** sufficient
  for distributed runs; the full-row sort is the load-
  bearing addition. The determinism gate explicitly allows
  `(TaskId → WorkerId)` to vary.
- **R5: Budget envelope underspec.** `QueryBudget` defaults
  are Phase 0 calibration; wrong values either reject every
  query (over-tight) or never enforce (over-loose).
  *Mitigation:* Phase 0 P0.6 calibration against the ADR 002
  bench fixtures; default values surfaced as configurable
  per deployment.
- **R6: Stage barrier latency cost.** Strict barriers add
  per-stage latency vs pipelined execution.
  *Mitigation:* V1 ships barrier-only; bench artifact records
  absolute latencies as diagnostic; V1.x pipelining ADR cites
  this evidence.
- **R7: M10 spill substrate API gap.** If the seed
  `SpillManager` doesn't expose enough for `PartitionAddress`-
  keyed lookup.
  *Mitigation:* Phase 0 P0.3 audit; extend `SpillManager`
  with `PartitionAddress` indexing in Phase 3 if needed.
- **R8: Heartbeat scaling.** Per-worker heartbeats at
  1-second cadence × N workers per coordinator scales
  linearly. Not a V1 problem (V1 is small clusters) but a
  future concern.
  *Mitigation:* document the scaling shape; V1.x
  WeakControlRing ADR addresses if needed.
- **R9: Cancellation grace window vs hard kill.** A worker
  that hangs after `CancelTask` blocks query termination.
  *Mitigation:* coordinator escalates `CancelTask` → mark
  `Lost` after grace window → query fails or proceeds with
  remaining workers per task FSM rules.
- **R10: Wire envelope size overhead.** Every message carries
  identity tuple + sequence + length; ~40 bytes fixed
  overhead per `TaskBatch`.
  *Mitigation:* acceptable for V1; future optimization via
  packed encoding if profiling shows overhead material.
- **R11: Worker registry static-vs-dynamic.** Static registry
  doesn't tolerate worker join/leave at runtime.
  *Mitigation:* V1 contract; WeakControlRing in V1.x via the
  same trait.
- **R12: Multi-process test environment setup.** CI workers
  may not permit `std::process::Command` + `SIGKILL`
  cleanly.
  *Mitigation:* Phase 0 P0.1 audit; if CI is hostile, gate
  the SIGKILL fixture behind a feature flag and document
  local-test requirement.
- **R13: ADR 002 `Coordinator` API churn.** ADR 002 shipped
  `Coordinator::admit_query` / `execute_query`; ADR 003
  extends with `fragment_plan` / `dispatch_loop` + a real
  task state machine.
  *Mitigation:* additive extension; ADR 002's existing
  `execute_query` becomes a single-task convenience wrapper
  over the new state machine. Tests stay green.
- **R14: ADR 002 storage-execution path regression.** ADR 003's
  wire / task / shuffle rewrites touch `coordinator.rs` and
  `server/worker.rs`, both of which carry ADR 002 PBC
  engagement and descriptor-pin behavior. A subtle refactor
  can break the storage contract without failing ADR 003 hard
  gates.
  *Mitigation:* the verification command list (Phase 10)
  keeps `cargo test -p toron --test architecture_test`
  (ADR 002 guards) and the live Wendelon-backed storage
  tests in the loop. Every ADR 003 phase commit re-runs the
  ADR 002 hard-gate test suite before merge.
- **R15: Wire envelope vs ADR 002 wire compatibility.** ADR
  003 introduces `WireEnvelope` with versioning; ADR 002
  didn't have one. Any ADR 002 wire test that encoded a raw
  `ToronMessage` will break.
  *Mitigation:* ADR 002 wire shape was in-process duplex
  only; rewriting those tests for the new envelope is Phase 2
  work. No persisted wire format to maintain compat with.
- **R16: Wire versioning grows compat scaffolding.** Single
  `protocol_version` field, reject mismatch.
  *Mitigation:* no legacy decoder, no reserved fields, no
  version-suffixed types. Rolling upgrades are V1.x
  territory.

## Definition of done

- All 9 Phase 0 pins recorded in the bench-results artifact.
- Phase 0 P0.8 `chore:` cleanup items land before Phase 1
  code.
- All 12 wire message kinds implemented split across two
  surfaces (9 task lifecycle coordinator-FSM + 3 data-movement
  W↔W partition-store protocol); seed `ToronMessage` retired;
  architecture guard G15 enforces `MorselComplete` absent.
- `TaskFailureKind` lands with 7 variants
  (`ProtocolMismatch`, `DescriptorEvolved`, `WorkerLost`,
  `TaskFailed`, `BudgetExhausted`, `Cancelled`, `Internal`);
  compile-time + grep guard enforces `PartitionMissing` /
  `StaleAttempt` are **not** present in `TaskFailureKind`.
  G15 ingress-side check enforces that `WorkerLost` never
  appears in a decoded wire `TaskFailed` payload — it is
  coordinator-synthesized into the FSM only.
- `PartitionManifest` carries `producer_worker_id` and
  `producer_endpoint`; `TaskInputBinding::ShuffleConsumer`
  carries materialized `Vec<ProducerPartitionRef>` resolved
  at consumer-task dispatch time. Worker `PullPartition`
  issuance does not require a registry round-trip.
- Producer-loss invalidation rule operational: a `Lost`
  producer's accepted manifests are invalidated, the
  per-attempt FSM record `Completed(AttemptId)` is retained
  for audit (that attempt did finish; the host died
  afterward), the acceptance index transitions
  `LiveAccepted(AttemptId) → Invalidated(AttemptId) → Open`,
  a fresh per-attempt FSM spawns with a bumped `AttemptId`
  and is redispatched; downstream consumers referencing
  invalidated manifests cancel or fail-retryable then
  redispatch after the new producer attempt completes.
- Accepted-attempt state indexed by `(QueryId, StageId,
  TaskId)` — never bare `TaskId`.
- `QueryId` migrated from `u128` to `Uuid` at a single
  canonical home (`src/distributed/identity.rs`); legacy
  callsites updated; no compat shim.
- `PartitionMissingReason` wire enum lands with four variants
  (`AttemptStale`, `CancelledTaskCleanup`, `WorkerLost`,
  `StoreEvicted`); coordinator-side
  `RuntimeError::PartitionStoreMismatch` lands as the pull-
  handler surface.
- `TaskIdentity` sub-struct lands with snake-cased `_id`-
  suffixed fields and is the canonical bundling in `TaskSpec`
  + every task-lifecycle wire message; no inline `task / attempt`
  field pairs in lifecycle message payloads.
- 10 distributed newtypes land (7 identity + 3 wire-level:
  `LeaseEpoch`, `FrameByteLen`, `StreamSeq`); architecture
  guard G9 enforces identity-tuple presence; G14 enforces
  `MorselId` worker-local.
- Wire envelope V1 implemented; architecture guard G8
  enforces.
- Pull-addressed partition store with in-memory + encrypted-
  spill backends; G11 enforces no P2P-push.
- `StaticWorkerRegistry` lands with `WorkerEntry` field set;
  trait carries lease / capability / resource fields for
  future WeakControlRing.
- Coordinator task FSM operational; `max_attempts = 2`
  enforced; G13 stage-barrier enforced.
- Worker fragment executor consumes `TaskSpec`; G12 enforces
  no worker-side `Binder::bind`.
- Per-query budget envelope enforced; G10 forbids
  `tokio::time::sleep` in production server / distributed
  paths.
- Cancellation propagation with grace window operational.
- SIGKILL keystone test passes: byte-identical output after
  **producer-worker kill** + manifest invalidation +
  producer-task redispatch + downstream consumer redispatch
  + ADR §13 canonical normalization (concat → stable lex
  full-row sort → reslice to 65,536-row batches → digest).
  Consumer-only kill is a separate easier fixture row.
- Cross-replica determinism gate passes: two independent runs
  (one with SIGKILL) digest-equal after the ADR §13
  normalization (concat → sort → reslice → digest), not
  ADR 002's concat-then-reslice-only normalization.
- Stale-attempt rejection fixture passes.
- `ExchangeId` manifest coverage fixture passes (hard gate
  #13).
- Registry availability fixture passes (hard gate #14):
  `Lost` worker is not selected for new attempts.
- All 14 hard promotion gates pass; soft gates met or
  explicitly justified (after Phase 0 P0.6 calibration).
- ADR 002 storage path remains regression-free (R14
  mitigation): every ADR 003 phase commit re-runs the
  ADR 002 hard-gate suite and Wendelon-backed live fixtures
  before merge.
- Bench artifact at
  `plans/impl/003_distributed_runtime_bench_results.md`
  records the full fixture matrix.
- Roadmap updated to record ADR 003 promotion and reframe
  ADR 004 scope as evidence-driven from bench results.
