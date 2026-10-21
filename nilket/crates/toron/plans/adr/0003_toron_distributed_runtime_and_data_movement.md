# ADR 003: Toron Distributed Runtime and Data Movement Contract

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Claude
**Companion implementation plan:**
`plans/impl/0003_toron_distributed_runtime_and_data_movement.md`

## Status

Promoted to `plans/adr/`. Pre-implementation; Phase 0
calibration runs first. Promotion evidence will live at
`plans/impl/0003_distributed_runtime_bench_results.md` after the
SIGKILL keystone fixture passes.

Builds on three topic-iteration rounds and four ADR/impl
iterations in `plans/wip/`. All three council voices reached
substantive convergence: remaining differences between drafts
were ordering and prose, not mechanism. Codex's evidence-first
reframing, Codex's pull-addressed shuffle and budget-envelope
framings, Gemini's "First Query with Integrity" V1 definition
carried forward, and Gemini's pull-addressing instinct and
rejected-alternatives framing are the load-bearing antecedents.

This ADR is Toron's V1 contract for **distributed execution
correctness**: how a bound plan gets fragmented, dispatched,
shuffled, retried, and reduced across multiple worker
processes, and how the coordinator observes the result with
at-most-once accepted-attempt semantics.

It does **not** decide distributed joins (ADR 004+), multi-
tenant resource governance (ADR 005+), operator-internal
streaming execution beyond a stream-shaped task output (V1.x),
pipelined cross-stage execution (V1.x), locality-aware fragment
routing (V1.x), or the WeakControlRing (Brainstorm 002).

## V1 contract summary

- **Dispatched unit of work is a bound `ExecutionPlan` chunk
  tagged with `(QueryId, StageId, TaskId, AttemptId)`.** No
  new IR level below ADR 002's `ExecutionPlan`. The coordinator
  binds once per query under the pinned
  `PhysicalDescriptorFingerprint`; workers execute the assigned
  chunk and never re-bind.
- **Distributed identity** carried on every wire message:
  `QueryId`, `StageId`, `TaskId`, `AttemptId`, `WorkerId`,
  `PartitionId`, `ExchangeId`. `MorselId` survives but is
  worker-local; it is not part of the distributed identity
  tuple and is architecture-guarded out of distributed
  payloads. Additional wire-level newtypes: `LeaseEpoch`
  (worker registry generation), `FrameByteLen` (envelope
  payload length), `StreamSeq` (per-stream sequence offset).
- **Pull-addressed partition store** for shuffle. Workers
  publish partitions keyed by `PartitionAddress { query_id,
  exchange_id, producer_stage_id, producer_task_id,
  attempt_id, partition_id }` with two backends: in-memory
  ephemeral and M10 encrypted local spill. Downstream tasks
  pull through a manifest published at producer task
  completion. No direct P2P push; no standalone shuffle
  service in V1.
- **Twelve wire-message kinds across two surfaces.** Nine
  task-lifecycle messages (`StartTask`, `TaskStarted`,
  `TaskBatch`, `TaskComplete`, `TaskFailed`, `CancelTask`,
  `TaskCancelled`, `Progress`, `Heartbeat`) constitute the
  coordinator-observed FSM. Three data-movement messages
  (`PullPartition`, `PartitionData`, `PartitionMissing`)
  constitute the W↔W shuffle protocol — addressed by
  `PartitionAddress`, not part of the task FSM. Seed
  `MorselComplete` / `Error(String)` retired.
- **Seven typed task-failure variants** in `TaskFailureKind`:
  `ProtocolMismatch`, `DescriptorEvolved`, `WorkerLost`,
  `TaskFailed { retryable }`, `BudgetExhausted { dimension }`,
  `Cancelled`, `Internal`. Partition-store wire has its own
  error surface: `PartitionMissingReason` (wire enum, four
  variants) carried on `PartitionMissing` responses, and
  `RuntimeError::PartitionStoreMismatch` on the coordinator
  side. Task-lifecycle and partition-store wire are kept as
  distinct error surfaces by design.
- **Per-query budget envelope** as the backpressure contract:
  bytes-in-flight per query / per worker stream, memory
  reserved per task, shuffle bytes published per task, spill
  bytes reserved per query, max Arrow IPC frame size, and
  cancellation cleanup obligations. `BudgetExhausted` failure
  on cap hit. **Not** multi-tenant governance.
- **Static `WorkerRegistry` trait** with a hand-configured
  `StaticWorkerRegistry` for V1. Carries `WorkerLease`,
  `WorkerCapabilities`, `WorkerResourceState`, and
  `WorkerStatus { Live | Suspect | Lost }`. The protocol
  carries enough lease / capability metadata that a future
  WeakControlRing can replace the registry implementation
  without wire breakage.
- **Versioned wire envelope** carrying `protocol_version`,
  `kind`, optional `identity: TaskIdentity`, optional
  `stream_seq: StreamSeq`, `payload_len: FrameByteLen`,
  optional `trace`. V1 rejects incompatible versions outright;
  no feature negotiation, no rolling upgrades.
- **Four-rule idempotence contract**: a task may run more than
  once; an attempt's output is idempotent under
  `(QueryId, StageId, TaskId, AttemptId, PartitionId)`; the
  coordinator accepts at most one attempt per *fully-qualified*
  task identity `(QueryId, StageId, TaskId)` (not bare `TaskId`,
  which is stage-local); partition assignment is deterministic
  across retries.
- **Producer-loss invalidation.** If a worker is marked `Lost`,
  the coordinator invalidates every accepted `PartitionManifest`
  it published and redispatches the affected producer-stage
  tasks with a bumped `AttemptId`. Producer-hosted partition
  state dies with the worker; the SIGKILL keystone covers this
  case explicitly.
- **Coordinator owns** binding, stage / task construction,
  worker assignment, completion tracking, retry, accepted-
  attempt state, cancellation propagation, and the per-query
  budget envelope. **Worker owns** local execution of assigned
  `TaskSpec`s, including worker-local morsel splitting.
- **Strict stage barriers in V1.** Producer stage publishes
  all partition manifests before consumer tasks dispatch.
  Pipelined cross-stage execution is V1.x.
- **Location-agnostic fragment routing in V1.** Workers are
  interchangeable; coordinator does not query Wendelon for
  ring placement. Locality-aware routing is V1.x.
- **Single retry per task default** (`max_attempts = 2`).
  Bounded retry count is part of the task FSM, not
  scattered constants.
- **SIGKILL keystone test** as the V1 hard gate:
  byte-identical normalized output after worker kill +
  redispatch.
- **Cross-replica determinism = output parity after canonical
  normalization.** Scheduler `(TaskId → WorkerId)` assignment
  may differ across runs (worker pool composition is non-
  deterministic in production). The ADR 003 canonical
  normalization (§13) is the pinned pipeline
  **concat → stable lex full-row sort → reslice (65,536-row
  batches) → digest** — sort *before* reslice, because
  reslicing first leaves cross-block row order non-
  deterministic. ADR 002's concat-then-reslice-only
  normalization is insufficient under distributed hash-
  aggregation, partition-arrival, and map-iteration
  nondeterminism. Determinism stops at the query output,
  not the scheduler internals.
- **Transport-agnostic contract.** ADR 003 is about lifecycle,
  addressing, and envelope, not the wire technology. V1
  implementation runs over real process endpoints — QUIC
  preferred if a clean dependency lands; framed-OS-socket
  through the existing `ToronTransport` trait acceptable if
  Phase 0 finds QUIC integration too expensive. The hard
  gate is "not duplex-only," not "QUIC specifically."

## Context

ADR 001 promoted Toron's foundation (greenfield Arrow-native
engine, `StorageScan` boundary). ADR 002 promoted the storage-
backed execution contract (PBC pushdown load-bearing, schema-
bound `ExecutionPlan`, descriptor-fingerprint pin, grouped
aggregate, structured errors, 10 hard gates passed).

The ADR 002 bench evidence
(`plans/impl/0002_storage_execution_contract_bench_results.md`)
proved Toron's smart-storage premise: `point_read_filter` at
10M rows runs at 39.35× speedup (252.7 MB → 1.2 KB on the
wire); `scan_filter` at 10M rows runs at 1.96× speedup. But
aggregate paths (`group_sum`, `sum_nullable`, `count_nullable`)
show PBC-disengaged at ~1.0× speedup — they flow through the
one-thread buffered executor at full input cardinality
because aggregate pushdown is explicitly out of ADR 002 scope.
Cross-replica determinism was explicitly deferred to ADR 003.

The remaining V1 gap is everything below ADR 002's
single-node single-process boundary:

- The seed's M5 / M6 used `tokio::io::duplex` in-process
  mocks. No multi-process transport, no real QUIC, no crash
  recovery, no shuffle persistence, no backpressure under
  load.
- `ToronMessage::ExecuteMorsel` ships a `LogicalPlan`, so
  each worker re-binds locally. Coordinator-owned binding
  from ADR 002 is bypassed across the wire.
- Worker messages lack query / stage / task / attempt
  identity. `MorselComplete` and stringified `Error(String)`
  are smoke-test-grade.
- Shuffle decodes payloads and drops them; nothing addresses
  partitions, nothing pulls them, nothing retries on
  producer death.

### Why distributed runtime is the keystone scope

Three alternative ADR 003 scopes were considered against the
ADR 002 bench evidence:

- **Executor model upgrade** (streaming end-to-end, morsel
  parallelism, push-based execution). Rejected: post-pushdown
  filter workloads are narrow (≤57% of baseline on selective
  scans, ~0.0005% on point reads), so the buffered executor
  isn't visibly groaning on filter paths. Aggregate paths are
  the bottleneck, but distributing aggregates via shuffle
  across workers solves them without an executor rewrite.
  Deferred to ADR 004+.
- **Catalog / schema registry hardening**. Rejected: ADR 002
  bench tested the descriptor-evolved retry path once and it
  worked. No evidence of high-frequency DDL pressure.
  Deferred to V1.x.
- **Distributed runtime and data movement** (this ADR).
  Selected: M5 / M6 mocks are the loudest remaining gap; the
  cross-ring single-node proof is the natural substrate to
  scale onto; cross-replica determinism is pre-committed.

### Why scope is intentionally narrow

ADR 003 is deliberately scoped to distributed-runtime
*mechanics* — task dispatch, shuffle, retry, lifecycle,
cancellation, backpressure budgets. It excludes:

- **Distributed joins.** Depend on shuffle (this ADR) +
  grouped aggregate (ADR 002 ✓), but join planning, broadcast
  vs hash, bloom pushdown, and skew handling are their own
  contract surface. ADR 004+.
- **Executor parallelism.** Morsel splitting within a task is
  worker-local implementation. Operator-internal streaming
  end-to-end is V1.x.
- **WeakControlRing.** HA coordinators / cluster heartbeats /
  shuffle-barrier sync via in-memory Raft is Brainstorm 002
  territory. ADR 003 carries lease / capability metadata in
  the wire envelope to make a future swap clean.
- **Multi-tenant resource governance.** Per-tenant CPU /
  memory / spill budgets, token-bucket admission, scheduler-
  slot allocation. ADR 005+.
- **Locality-aware fragment routing.** Would require new
  Wendelon public-API surface; defer until V1.x evidence
  shows location-agnostic routing is the bottleneck.
- **Pipelined cross-stage execution.** Strict stage barriers
  in V1; pipelining when the failure-recovery story for
  pipelined shuffle is concrete (V1.x).
- **RDMA / RoCEv2 / SIW transport.** QUIC preferred through
  V1; RDMA seam ADR is V1.x.

## Decision

### 1. Unit of work: tagged `ExecutionPlan` chunks (`TaskSpec`)

The coordinator binds the `LogicalPlan` once per query
(ADR 002 §1) producing an `ExecutionPlan`. The coordinator
then cuts the plan into stage- and task-sized chunks; each
chunk is wrapped in a `TaskSpec`:

```rust
pub struct TaskIdentity {
    pub query_id: QueryId,
    pub stage_id: StageId,
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
}

pub struct TaskSpec {
    pub identity: TaskIdentity,
    pub partition: Option<PartitionId>, // Some(_) for shuffle producer/consumer
    pub plan: ExecutionPlan,             // the bound chunk
    pub inputs: Vec<TaskInputBinding>,   // upstream partition refs
    pub output: TaskOutputBinding,       // where to publish
    pub budget: TaskBudget,              // per-task envelope slice
    pub trace: Option<TraceContext>,
}

pub enum TaskInputBinding {
    Scan,                                 // root scan, no upstream
    ShuffleConsumer {
        exchange_id: ExchangeId,
        producer_stage_id: StageId,
        partition_id: PartitionId,
        // Materialized at consumer-task dispatch time from the
        // coordinator-accepted producer manifests. Each ref
        // carries everything the worker needs to issue
        // `PullPartition` without consulting the registry.
        producer_partitions: Vec<ProducerPartitionRef>,
    },
}

pub struct ProducerPartitionRef {
    pub address: PartitionAddress,        // full pull key
    pub worker_id: WorkerId,              // accepted-attempt host
    pub endpoint: WorkerEndpoint,         // wire endpoint
    pub size_bytes: u64,
    pub digest: ArrowIpcDigest,
}

pub enum TaskOutputBinding {
    FinalResult,                          // streams to coordinator
    ShuffleProducer {
        exchange_id: ExchangeId,
        partition_count: u32,
        partition_key: Vec<ExecutionExpr>,
    },
}
```

No new IR level below `ExecutionPlan`. The chunk granularity
is **one task per shuffle partition per stage**; for non-
shuffle stages, one task per `Scan` source split.

### 2. Distributed identity newtypes

All per the `newtype-units` convention:

```rust
// Distributed identity tuple (7)
pub struct QueryId(Uuid);              // UUIDv7, coordinator-assigned, lifetime = query
pub struct StageId(u32);                // one per stage in the DAG
pub struct TaskId(u32);                 // one per partition within a stage
pub struct AttemptId(u32);              // bumped on redispatch
pub struct WorkerId(Uuid);              // UUIDv7, assigned by WorkerRegistry
pub struct PartitionId(u32);            // shuffle partition index
pub struct ExchangeId(u32);             // one per shuffle boundary in DAG

// Wire-level newtypes (3)
pub struct LeaseEpoch(u32);             // worker registry lease generation
pub struct FrameByteLen(u32);           // envelope payload length
pub struct StreamSeq(u64);              // per-stream sequence offset
```

`MorselId` (ADR 001) survives as a worker-local scheduling
detail; it does **not** appear in `TaskSpec`, `TaskOutputBinding`,
the partition store key, or any wire message.

Distributed addressing keys off `(QueryId, StageId, TaskId,
AttemptId)`, plus `PartitionId` for shuffle artifacts.
Idempotence key is `(QueryId, StageId, TaskId, AttemptId)`
— never bare `(TaskId, AttemptId)`, because `TaskId` is
stage-local and would collide across stages of the same
query.

### 3. Wire format: postcard control + Arrow IPC data + versioned envelope

Format choice (unchanged from ADR 002 M5 seed shape):
postcard for control messages, Arrow IPC for record batches.
Rejected alternatives: unified frame format (no V1 benefit),
Arrow Flight (gRPC dependency).

**Versioned envelope** wraps every wire message:

```rust
pub struct WireEnvelope {
    pub protocol_version: ProtocolVersion,   // newtype u16; V1 = 1
    pub kind: MessageKind,                    // enum tag
    pub identity: Option<TaskIdentity>,       // lifecycle messages
    pub stream_seq: Option<StreamSeq>,        // stream offset / batch index
    pub payload_len: FrameByteLen,
    pub trace: Option<TraceContext>,
    pub payload: MessagePayload,
}
```

V1 rejects incompatible `protocol_version` with typed
`RuntimeError::ProtocolMismatch`. No feature negotiation, no
rolling upgrades, no cross-version compat. Max-frame
validation uses `FrameByteLen` against the per-query budget
envelope before allocation.

### 4. Wire message kinds

Twelve message kinds split across two surfaces. **Task
lifecycle** (9) is the coordinator-observed FSM. **Data
movement** (3) is the W↔W shuffle protocol — partition-store
traffic addressed by `PartitionAddress`, not coordinator-
observed task states.

```rust
pub enum MessageKind {
    // Task lifecycle (9) — coordinator-observed FSM
    StartTask,        // C → W: dispatch a TaskSpec
    TaskStarted,      // W → C: acknowledged, executing
    TaskBatch,        // W → C: incremental Arrow IPC batch
    TaskComplete,     // W → C: task finished successfully
    TaskFailed,       // W → C: task hit a typed failure (one of six worker-emittable variants;
                      //         WorkerLost is coordinator-synthesized only — see §4)
    CancelTask,       // C → W: cancel an in-flight task
    TaskCancelled,    // W → C: cancellation acknowledged + cleaned up
    Progress,         // W → C: optional rate / budget update
    Heartbeat,        // both directions: liveness signal

    // Data movement (3) — W ↔ W shuffle protocol, not task FSM
    PullPartition,    // W → W: consumer requests a partition by address
    PartitionData,    // W → W: producer ships an Arrow IPC partition
    PartitionMissing, // W → W: producer reports the partition is absent
                      //         (cleaned up / unaccepted / stale attempt)
}
```

Lifecycle messages carry `TaskIdentity`; data-movement
messages carry `PartitionAddress`. `Heartbeat` carries
`(WorkerId, LeaseEpoch)` only.

`TaskFailed` carries one of seven typed failure variants:

```rust
pub enum TaskFailureKind {
    ProtocolMismatch,                            // wire envelope incompatible
    DescriptorEvolved {
        expected: PhysicalDescriptorFingerprint,
        current: PhysicalDescriptorFingerprint,
    },
    WorkerLost { worker_id: WorkerId },          // see note: coordinator-synthesized only
    TaskFailed { reason: String, retryable: bool },
    BudgetExhausted {
        dimension: BudgetDimension,
        requested: u64,
        available: u64,
    },
    Cancelled,
    Internal { reason: String },
}
```

`BudgetExhausted` retryability depends on the dimension
(memory → retryable on different worker; spill-bytes-per-query
→ terminal). `TaskFailed { retryable }` is the only variant
with an explicit retryability flag.

**`WorkerLost` is coordinator-synthesized only.** A worker that
has lost contact cannot emit `TaskFailed` over the wire. The
variant exists in the unified `TaskFailureKind` so the
coordinator's task FSM can record cause uniformly, but
`TaskFailed { kind: WorkerLost, ... }` is **never** an actual
wire message — it is written into the FSM by the coordinator
when the `WorkerRegistry` transitions the host to `Lost`
(heartbeat timeout, transport drop, or lease expiry). Six
variants are worker-emittable on the wire; one
(`WorkerLost`) is coordinator-internal. Architecture guard G15
forbids `WorkerLost` from appearing in any encoded
`TaskFailed` payload received at the coordinator's wire
ingress.

**Distinct error surface — partition store / shuffle:**
`PartitionMissing` is a *wire response* to `PullPartition`,
not a `TaskFailed` variant. It carries:

```rust
pub enum PartitionMissingReason {
    AttemptStale {                                  // wrong AttemptId requested
        expected_attempt: AttemptId,
        observed_attempt: AttemptId,
    },
    CancelledTaskCleanup,                           // partition cleaned up after cancel
    WorkerLost,                                     // producer never published
    StoreEvicted,                                   // promoted past budget, then dropped
}
```

Stale-attempt rejection at the producer's local
`PartitionStore` surfaces as `PartitionMissing { reason:
AttemptStale, ... }` on the wire and as a coordinator-side
`RuntimeError::PartitionStoreMismatch` in the consumer's pull
handler. The consumer reissues the pull against the
coordinator-accepted attempt; if the coordinator's accepted
attempt has rolled forward beyond what the consumer knows,
the consumer task fails with `TaskFailureKind::TaskFailed
{ retryable: true, reason: "stage barrier resync" }`.

This split keeps task-failure semantics (what the coordinator
FSM acts on) cleanly separated from partition-store wire
semantics (what producer-consumer worker pairs negotiate).

### 5. Shuffle: pull-addressed partition store

Workers publish shuffle artifacts to a per-worker partition
store. Addressing key:

```rust
pub struct PartitionAddress {
    pub query_id: QueryId,
    pub exchange_id: ExchangeId,
    pub producer_stage_id: StageId,
    pub producer_task_id: TaskId,
    pub attempt_id: AttemptId,
    pub partition_id: PartitionId,
}
```

After producer task completion, the worker publishes a
**partition manifest** to the coordinator:

```rust
pub struct PartitionManifest {
    pub query_id: QueryId,
    pub stage_id: StageId,
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
    pub exchange_id: ExchangeId,
    pub producer_worker_id: WorkerId,            // host of the partition store
    pub producer_endpoint: WorkerEndpoint,        // pull target
    pub partitions: Vec<PartitionEntry>,
    pub backend: PartitionBackend,
}

pub struct PartitionEntry {
    pub partition_id: PartitionId,
    pub size_bytes: u64,
    pub row_count: u64,
    pub digest: ArrowIpcDigest,                  // for stale-attempt detection
}

pub enum PartitionBackend {
    InMemory,                                    // V1 default
    EncryptedSpill { spill_id: SpillId },        // M10 substrate
}
```

Downstream consumer tasks (whose `TaskInputBinding ==
ShuffleConsumer { ... }`) issue `PullPartition` requests
against producer workers; producers serve from their local
backend (in-memory or encrypted spill).

**Backend selection:** in-memory by default. The producer
worker promotes to encrypted spill when its `shuffle bytes
published per task` budget crosses a high-water threshold OR
when the consumer stage hasn't started within a configured
window (anticipatory durability for retry across producer
restart). V1 default thresholds:

- spill if producer holds > 64 MiB unconsumed partitions, OR
- spill if producer task completed > 5 seconds before
  consumer start.

Both thresholds are Phase 0 P0.6 calibration items.
Promotion is **one-way within a task**: once a task spills a
partition set, it does not move that partition set back to
memory.

**No direct P2P push.** The producer never initiates delivery
to a consumer; consumers always pull. Retry-clean,
backpressure-clean.

**No standalone shuffle service.** Each producer hosts its
own partition store. A future ADR (replicated / disaggregated
shuffle service) is V2 territory.

### 6. Per-query budget envelope

The coordinator allocates a per-query envelope at admission:

```rust
pub struct QueryBudget {
    pub bytes_in_flight_per_query: u64,          // global cap on outstanding wire bytes
    pub bytes_in_flight_per_worker_stream: u64,  // per worker channel
    pub memory_per_task_bytes: u64,              // working-set cap
    pub shuffle_bytes_published_per_task: u64,   // before spill triggers
    pub spill_bytes_reserved_per_query: u64,     // total NVMe budget
    pub max_arrow_ipc_frame_bytes: u32,          // per TaskBatch payload
    pub cancellation_grace_ms: u32,              // worker cleanup window
}
```

When a task cannot reserve memory, wire capacity, frame
capacity, or spill space, it emits `TaskFailed
{ kind: BudgetExhausted, ... }`. The coordinator decides
retryability based on the `BudgetDimension`:

- `Memory` / `ShuffleBytes` → retryable (different worker
  may have headroom).
- `SpillBytes` / `BytesInFlightPerQuery` → terminal
  (query-level cap exhausted).
- `FrameSize` → terminal. A frame exceeding
  `max_arrow_ipc_frame_bytes` is a producer-side authoring
  problem (the producer emitted an IPC frame larger than
  the per-query cap); retrying the same task on a different
  worker would emit the same oversize frame against the
  same per-query budget. V1 has no frame-splitting fallback
  — terminal-fail is the contract; V1.x can revisit if
  evidence shows large-frame workloads matter.

V1 budget is per-query only. Per-tenant budgets, admission
control, and scheduler-slot allocation are ADR 005+.

### 7. Worker registry

```rust
pub trait WorkerRegistry: Send + Sync {
    fn list(&self) -> Vec<WorkerEntry>;
    fn resolve(&self, worker_id: WorkerId) -> Option<WorkerEntry>;
    fn select_for_attempt(&self, hints: &PlacementHints) -> Option<WorkerId>;
    fn mark_lost(&self, worker_id: WorkerId);
    fn heartbeat(&self, worker_id: WorkerId, lease: WorkerLease);
}

pub struct WorkerEntry {
    pub worker_id: WorkerId,
    pub endpoint: WorkerEndpoint,
    pub lease: WorkerLease,                       // last_heartbeat + epoch
    pub capabilities: WorkerCapabilities,         // mem cap, SIMD width, etc.
    pub resources: WorkerResourceState,           // coarse live load
    pub status: WorkerStatus,                     // Live | Suspect | Lost
}

pub enum WorkerStatus { Live, Suspect, Lost }
```

V1 implementation: `StaticWorkerRegistry` — hand-configured
worker list, no heartbeat-driven addition; the coordinator
marks `Suspect` after the first missed heartbeat and `Lost`
after the threshold or stream failure. Lost workers are not
selected for new attempts. Future WeakControlRing
implementation slots into the same trait.

### 8. Idempotence contract

Four rules:

1. A task may run more than once.
2. A task attempt may publish output more than once only if
   the publish key is idempotent — same `(QueryId, StageId,
   TaskId, AttemptId, PartitionId)` tuple yields the same
   partition bytes after the §13 canonical normalization
   pipeline.
3. The query result observes **at most one accepted attempt
   per fully-qualified task identity**. The coordinator
   indexes accepted-attempt state by `(QueryId, StageId,
   TaskId)` — *not* bare `TaskId`, which is stage-local —
   and picks the first `TaskComplete` it receives per
   `(QueryId, StageId, TaskId)`. Subsequent completions are
   stale and rejected.
4. Partition assignment is **deterministic across retries** —
   the same `(QueryId, StageId, TaskId)` always maps to the
   same `PartitionId` set regardless of which attempt
   produced the output.

**Coordinator-side state has two layers.** The per-attempt
FSM and the per-task acceptance index are deliberately
separated so that "this attempt finished" and "this task is
satisfied" are independent facts. Conflating them was the
root of finding 1 in the iter-4 review (a `Completed` attempt
appeared to re-enter `FailedRetryable`, which contradicts the
attempt's own terminal status).

**Per-attempt FSM** — keyed by
`(QueryId, StageId, TaskId, AttemptId)`. Terminal states are
terminal *for that attempt*, not for the task.

```
Pending
  → Dispatched(AttemptId)
    → Running(AttemptId)
      → Completed(AttemptId)               [attempt-terminal, see acceptance layer]
      → FailedRetryable(AttemptId)         [attempt-terminal; task may spawn next attempt]
      → FailedTerminal(AttemptId)          [attempt-terminal; task fails the query]
      → Cancelled(AttemptId)               [attempt-terminal, cleanup]
```

**Per-task acceptance index** — keyed by
`(QueryId, StageId, TaskId)`. Tracks whether the task is
satisfied by some attempt and whether the satisfying attempt
is still usable.

```
Open                                       [no accepted attempt yet]
  → LiveAccepted(AttemptId)                [an attempt has Completed and is hosted on a Live worker]
    → Invalidated(AttemptId)               [accepted attempt's host is now Lost; partitions died with it]
    → Open                                 [via Invalidated → spawn next attempt; loop until task fully satisfied or budget exhausted]
  → Failed                                 [some attempt hit FailedTerminal OR max_attempts exhausted]
  → Cancelled                              [query cancelled]
```

A task is **fully satisfied** when its acceptance index is
`LiveAccepted(AttemptId)` AND (for producer-stage tasks) the
producer is `Drained`. Once a producer-stage task is fully
satisfied, the partition state is no longer needed for
query correctness. Stage-barrier dispatch (§10) reads the
acceptance index, not the per-attempt FSM directly.

**`Drained` definition.** A producer-stage task is `Drained`
when **every consumer task that referenced its partitions
has itself reached `LiveAccepted(AttemptId)` in the
acceptance index** — not merely "every consumer has finished
pulling." Pull completion alone is too early: a consumer can
finish pulling, then fail before its own `TaskComplete` is
accepted (OOM during final aggregate, descriptor evolution,
etc.). If the producer worker were lost after pull-ack but
before downstream acceptance, the consumer retry would have
no producer-hosted partition data to repull. Tying `Drained`
to consumer acceptance (not consumer pull completion) keeps
the producer rerunnable until every downstream attempt that
might need its data is itself terminally satisfied.

For final-result consumer tasks, "`LiveAccepted`" means the
consumer's `TaskComplete` has been accepted by the
coordinator (the consumer's own first-win acceptance per
rule 3). For intermediate-stage consumers, `LiveAccepted`
propagates upward via the same rule: a producer is
`Drained` only when its consumers are also `Drained`-or-
final-`LiveAccepted`. The graph terminates at the query
result, so `Drained` is well-defined.

**Producer-loss invalidation rule.** When the
`WorkerRegistry` transitions a worker `Live`/`Suspect` →
`Lost`, the coordinator:

1. Walks the acceptance index for all producer-stage tasks
   whose `LiveAccepted(AttemptId)` was hosted on that worker
   **AND that are not yet `Drained`** (per the stricter
   definition above: every consumer that referenced these
   partitions has reached `LiveAccepted` in its own
   acceptance index, transitively up to the query result).
   Tasks that are already `Drained` are skipped: no
   downstream attempt can still need their partition data,
   so partition state going away is harmless. Rerunning a
   `Drained` producer would be wasted work and, worse,
   would invalidate consumer attempts that have already
   legitimately observed the original output.
2. Invalidates each affected (still-needed) `PartitionManifest`.
   The producer-hosted partition state died with the worker
   (V1 stores are not replicated). The acceptance index for
   each affected task transitions
   `LiveAccepted(AttemptId)` → `Invalidated(AttemptId)`.
   **The per-attempt FSM for that `AttemptId` stays
   `Completed`** — that attempt did finish, the
   coordinator did accept it, the producer host then died
   later. Those are three different facts.
3. The task transitions `Invalidated(AttemptId)` → `Open`
   and spawns a fresh per-attempt FSM in `Pending` with a
   bumped `AttemptId`. The original `Completed(AttemptId)`
   record is retained for audit / debug; new pulls cannot
   target it.
4. Consumers whose `producer_partitions` referenced an
   invalidated manifest are either (a) cancelled and
   redispatched after the producer rerun lands, if they
   haven't yet started, or (b) fail with
   `TaskFailureKind::TaskFailed { retryable: true,
   reason: "producer manifest invalidated" }` if mid-pull,
   then redispatched after the producer rerun.

This is what makes the SIGKILL keystone work when the killed
worker is a producer holding accepted partitions. The keystone
fixture (§12) covers the producer-kill case explicitly;
consumer-kill is the easier sub-case.

Single retry default per task (`max_attempts = 2`) — applies
to the acceptance index, not the per-attempt FSM. After the
second `Invalidated` or `FailedRetryable` spawn, the task
moves to `Failed` and the query fails. Bounded retry count
lives in `RetryPolicy`, not scattered constants.

**Stale partition rejection:** consumer tasks include the
expected `AttemptId` in their `PullPartition` request; the
producer rejects with `PartitionMissing { reason:
AttemptStale, ... }` if its current accepted attempt doesn't
match.

### 9. Scheduler ownership

**Coordinator owns:**

- Binding `LogicalPlan` → `ExecutionPlan` via ADR 002 binder
  and pinned fingerprint.
- Building stages from the bound plan; cutting tasks per
  partition.
- Assigning `TaskId → WorkerId` via the `WorkerRegistry`.
- Tracking the task state machine.
- Tracking partition-manifest publication and downstream
  readiness.
- Redispatching `FailedRetryable` tasks with bumped
  `AttemptId`.
- Accepting at-most-one attempt per task.
- Allocating and enforcing the per-query budget envelope.
- Cancellation propagation.

**Worker owns:**

- Executing the assigned `TaskSpec.plan` (an `ExecutionPlan`
  chunk).
- Local morsel scheduling (worker-internal `MorselId`).
- Publishing result batches via `TaskBatch` for final-result
  tasks, or partition artifacts to the local store for
  shuffle producers.
- Serving `PullPartition` requests for its hosted partitions.
- Sending `Heartbeat` / `Progress` / `TaskComplete` /
  `TaskFailed` per lifecycle.
- Local budget tracking; emitting `BudgetExhausted` on
  reservation failure.

**V1 scheduler is intentionally plain:**

- Static worker list.
- Round-robin or least-loaded task placement based on
  `WorkerResourceState`.
- `max_attempts = 2` per task.
- Deterministic partition mapping: `partition_id =
  hash(partition_key_columns) mod partition_count`. Hash
  function is stable across retries (V1: `xxh3_64`).
- No cost model; no statistics-driven planning.

### 10. Strict stage barriers V1

The coordinator dispatches Stage N+1 tasks only after **all**
Stage N producers have published their partition manifests
and the coordinator has accepted the winning attempt for each.
Within a stage, tasks may run in parallel.

Justification:

- Producer retry does not race with downstream consumption.
- Stale-attempt rejection happens at manifest / partition
  boundaries, not mid-stream.
- Cancellation cleanup verifies stage-by-stage.
- SIGKILL kill-mid-query fixture has fewer timing-dependent
  failure modes.

Pipelined cross-stage execution is V1.x; it requires a
distinct failure-recovery story that V1 doesn't have to
solve.

### 11. Location-agnostic fragment routing V1

Workers are interchangeable from the coordinator's
perspective. The coordinator does **not** query Wendelon for
ring placement, row-group locality, or table-object
co-location.

This is a deliberate deferral, not a rejection. Locality-
aware fragment routing matters for production efficiency,
but it would pull new Wendelon public-API surface into ADR
003 (which the import allowlist forbids) and conflate
ADR 003's distributed-runtime identity decisions with a
storage-side coordination problem. V1.x territory.

### 12. Crash-only fault tolerance (the SIGKILL keystone)

The ADR 001 §M5 hard gate, deferred through ADR 002, lands
here as the keystone test:

1. Coordinator dispatches a multi-stage query (Scan + Filter +
   Aggregate + GROUP BY + Shuffle + Final Aggregate) across
   N ≥ 2 worker processes.
2. Mid-shuffle (after at least one producer manifest has been
   accepted by the coordinator, during consumer pull), the
   test harness `SIGKILL`s **a worker hosting an accepted
   producer manifest** — i.e., the harder case where
   producer-hosted partition state dies with the worker.
   (Consumer-only kill is a strictly easier sub-case and is
   covered as a separate fixture row.)
3. Coordinator detects via transport stream drop on the
   worker's wire, or via heartbeat timeout (V1 default: 3
   missed heartbeats at 1-second cadence).
4. Coordinator applies the **producer-loss invalidation rule**
   (§8): for each affected producer task, the per-attempt
   FSM record `Completed(AttemptId)` is retained (the
   attempt did finish, the host died later — that record
   stays for audit), the acceptance index transitions
   `LiveAccepted(AttemptId) → Invalidated(AttemptId) →
   Open`, a fresh per-attempt FSM spawns in `Pending` with
   a bumped `AttemptId`, and the new attempt is dispatched
   to a live worker. Consumer-stage tasks that referenced
   invalidated manifests are also redispatched once the new
   producer attempts complete (per §8 rule).
5. Query completes. Final Arrow IPC output after the §13
   canonical normalization (concat → stable full-row lex
   sort → reslice to 65,536-row batches → digest) is
   **byte-identical** to a no-failure baseline run of the
   same query.

This is hard gate #4 below.

### 13. Cross-replica determinism gate

Two independent multi-worker runs of the same query produce
byte-identical Arrow IPC output after canonical normalization.
One run includes a worker SIGKILL mid-shuffle.

**Canonical normalization for ADR 003** — exact pipeline in
this order:

1. **Concat** all output batches into a single logical
   record-batch sequence (no slicing yet).
2. **Stable lexicographic sort over the full row** — all
   columns left-to-right, deterministic null ordering
   (e.g., nulls-last consistently), stable to preserve
   relative order of equal-key rows.
3. **Reslice** to fixed 65,536-row batches (ADR 002's
   reslice width).
4. **Digest** the resulting Arrow IPC byte stream.

The order matters: sorting **before** reslice ensures the
row-order canonicalization isn't undone by sort-output
chunking. Reslicing first and then sorting per-batch would
re-introduce batch-boundary ambiguity because sort would only
canonicalize within each 65,536-row block, not across blocks.

The full-row sort is required because:

- Hash aggregation produces group output in hash-bucket
  iteration order, which is non-deterministic across runs
  even within a single worker.
- Partition arrival at consumer workers depends on producer
  completion timing, which varies across runs.
- Map / `HashMap` iteration order is intentionally
  non-deterministic in Rust.

Concat-then-reslice alone canonicalizes batch boundaries but
not row order; trying to gate on byte-identical-without-sort
would fail on every distributed run. The full-row sort is the
weakest normalization that survives all three non-determinism
sources above while still catching real correctness bugs
(value drift, lost rows, duplicated rows).

The helper is the same `canonical_normalize` API used by
ADR 002 parity tests, extended with a `sort: SortPolicy`
parameter; ADR 002 callsites pass `SortPolicy::None`,
ADR 003 callsites pass `SortPolicy::FullRowLex`.

Scheduler `(TaskId → WorkerId)` assignment **may differ**
between the two runs (worker pool composition is non-
deterministic in production; workers join / leave
independently). Determinism stops at the query output, not
the scheduler internals.

Hard gate #11 below.

## Rationale

### Why no new IR level below `ExecutionPlan`

`ExecutionPlan` is already physical-ish — bound, typed, with
PBC programs attached and projection ordinals resolved against
the pinned descriptor. A new IR level for "stage- and task-
sized chunks" adds an IR transformation pass for no semantic
benefit. Stage boundaries are tags on `ExecutionPlan` nodes;
task boundaries are partition shards of those tags. A
`TaskSpec` wrapper around an `ExecutionPlan` chunk carries the
metadata workers need without recursive transformation. If
implementation reveals that `ExecutionPlan` chunks are too
coarse, the ADR may define helper structs around the chunk —
but not a competing plan vocabulary.

### Why pull-addressed partition store

Three V1 candidates were considered:

- **Direct P2P push** (seed M6 shape). Producer streams
  directly into consumer's QUIC stream. Fast on happy path
  but fragile: receiver death stalls the producer, retry
  requires re-running both sides, stale-attempt detection is
  implicit.
- **Pure spill-to-pull**. Every producer writes to encrypted
  spill; consumer pulls from spill. Maximum resilience but
  pays disk-write + decrypt latency on every shuffle,
  including queries that fit in RAM.
- **Pull-addressed with two backends** (adopted). Producer
  publishes to local in-memory store by default; promotes to
  encrypted spill when budget / staleness thresholds trigger.
  Consumer always pulls via the partition address.

The pull-addressed shape honors the pull-addressing instinct
(retry-clean, addressable artifacts) without the spill-on-
every-shuffle cost. The in-memory backend serves the happy
path; the spill backend serves resilience.

### Why per-query budget envelope, not transport flow control alone

Transport-native backpressure handles bytes-on-wire, not
application-level resources (memory headroom, spill space,
frame-size caps). A receiver running out of memory budget
can't tell the sender via flow control to "stop publishing
partitions" — it can only stop accepting bytes for the
specific stream. Without an application-level budget, a fast
sender + slow receiver under multi-stream shuffle can
independently fill N receive buffers, each consuming heap.

The budget envelope is the **contract**; transport flow
control is the **wire mechanism**. They compose: application-
level reservation prevents over-publish; transport
backpressure prevents in-flight overrun. Both required.

### Why stage barriers in V1

Pipelined cross-stage execution would let Stage N+1 consume
Stage N's output as it arrives, reducing query latency. But it
complicates:

- **Producer retry recovery.** If a Stage N task fails after
  Stage N+1 has consumed partial output, Stage N+1 must
  either roll back consumed state or detect that its upstream
  changed mid-flight.
- **Stale-attempt rejection.** Mid-stream attempt invalidation
  requires consumers to abandon in-progress work.
- **Cancellation propagation.** Mid-stream cancellation must
  drain in-flight stages cleanly.

V1's strict barrier avoids all three. The latency cost is real
but acceptable for a V1 that prioritizes correctness over
throughput. V1.x can add pipelining once V1's fault-tolerance
story is concrete.

### Why location-agnostic V1

Locality-aware routing — coordinator queries Wendelon for
table row-group placement and routes fragments to co-located
workers — would deliver real perf wins on large scans. But it:

- Requires new Wendelon public-API surface for ring-placement
  lookup. Conflict with ADR 002's import allowlist
  (`wendelon::network::raft::*` forbidden).
- Conflates ADR 003's runtime-identity contract with a
  storage-side coordination problem.
- Doesn't compose cleanly with future WeakControlRing
  (which itself owns coordinator-worker placement).

V1.x territory once V1 baseline distributed runtime is proven.

### Why single retry default

V1 default `max_attempts = 2` (one retry). Matches ADR 002
fingerprint-pin retry shape — one retry then fail. Higher
retry counts buy resilience at the cost of:

- Longer query tails when retries chain.
- Harder reasoning about deterministic output (each retry is
  a new attempt; cross-replica determinism gate gets noisier).
- More opportunity for cascading failures (retried task hits
  same OOM on a different worker).

V1.x can raise retry count with evidence of which failure
modes benefit.

### Why output determinism, not assignment determinism

Worker pools are dynamic. Stable worker assignment is not a
useful contract when workers can join, leave, or fail. Query
output is the user-visible invariant; scheduler placement is
an implementation detail.

### Why transport-agnostic

ADR 003 is about task identity, lifecycle, partition
addressing, retry, and budgets. QUIC is the preferred
implementation transport, but the contract should not turn
the transport choice into the architectural center. The
promotion gate is real process endpoints, not
`tokio::io::duplex`. A framed OS-socket transport through
`ToronTransport` is acceptable if Phase 0 finds QUIC
integration too expensive for the first runtime slice. RDMA
and zero-copy remain later transport optimizations over the
same envelope.

## Consequences

**Positive:**

- The coordinator / worker boundary becomes an enforceable
  runtime contract.
- Retry and stale-output rejection are explicit rather than
  inferred from dropped streams.
- Shuffle correctness can be tested before distributed joins
  arrive.
- Worker membership has a concrete seam without committing to
  WeakControlRing.
- Resource failures become typed and observable.

**Costs:**

- `src/wire.rs`, `src/server/mod.rs`, and `src/coordinator.rs`
  need substantial rewrites.
- The executor must accept bound task fragments without
  rebinding on workers.
- The partition store introduces new lifecycle cleanup
  obligations.
- Strict stage barriers limit performance until pipelining is
  designed.
- The first worker registry is intentionally simple and not
  HA.

## Promotion criteria

Bench artifact records hard / soft gates. Hard gates block
promotion; soft gates can fail with recorded justification
(Wendelon ADR 070 precedent).

### Hard gates

1. **Multi-process execution.** A coordinator executes a
   multi-task plan against at least two real worker processes,
   not `tokio::io::duplex`.
2. **Bound fragment dispatch.** Workers receive `TaskSpec`
   wrapping `ExecutionPlan` chunks; no worker re-binds a
   `LogicalPlan`.
3. **Identity-tagged lifecycle.** Every distributed wire
   message carries `(QueryId, StageId, TaskId, AttemptId)`
   in `TaskIdentity` (lifecycle) or `PartitionAddress` (data
   movement). **Exception:** `Heartbeat` carries
   `(WorkerId, LeaseEpoch)` only — it is per-worker, not
   per-task, and predates task assignment. Architecture-
   guarded G9.
4. **SIGKILL keystone — producer-loss variant.** A
   SIGKILL'd worker that hosts an **accepted producer
   manifest** mid-shuffle triggers the producer-loss
   invalidation rule (§8): manifest invalidated, producer
   task redispatched with new `AttemptId`, downstream
   consumers re-pull against the new attempt; final Arrow
   digest equals the no-failure baseline after the §13
   canonical normalization (concat → full-row sort → reslice
   → digest). Consumer-kill is covered as an easier
   separate row.
5. **Stale-output rejection.** A fixture where a worker
   produces output from a stale `AttemptId` sees the
   coordinator reject it; accepted output count remains 1
   per task.
6. **Pull-addressed shuffle observed.** Shuffle traffic uses
   `PullPartition` requests addressed by `PartitionAddress`;
   no decoded-and-dropped shuffle path remains in runtime
   code.
7. **Deterministic partition assignment.** Two sub-gates,
   reflecting the scheduler rule `partition_id =
   xxh3_64(partition_key_columns) mod partition_count` (§9),
   which does not depend on `QueryId`:
   - **Within a query (retry stability):** for a single
     query, the same `(exchange_id, partition_count,
     partition_key_values)` always maps to the same
     `PartitionId` regardless of which `AttemptId` produced
     the partition. This is what idempotence rule 4
     depends on.
   - **Across runs (logical-plan stability):** for two
     independent runs of the same `LogicalPlan` against the
     same descriptor fingerprint, the same
     `(logical_exchange_position_in_plan, partition_count,
     partition_key_values)` maps to the same `PartitionId`.
     Per-run `QueryId` differs (UUIDv7); per-run
     `ExchangeId` may differ if assignment isn't structural,
     so the gate compares logical-plan position, not the
     run-scoped `ExchangeId`. Verified by digest pair.
8. **Cancellation correctness.** A cancelled query releases
   all task memory / shuffle / spill reservations; subsequent
   output from cancelled tasks is rejected.
9. **Typed wire errors.** `TaskFailed` carries a structured
   `TaskFailureKind`; no `Error(String)` appears in any
   distributed-runtime module's public API. Architecture-
   guarded.
10. **Backpressure without sleep.** A fixture with bounded
    receive buffers proves the per-query budget envelope
    enforces a byte cap; no `tokio::time::sleep` mock appears
    in the production worker path. Architecture-guarded.
11. **Cross-replica determinism.** Two independent multi-
    worker runs (one with a SIGKILL) produce byte-identical
    Arrow IPC output after the §13 canonical normalization
    (concat → stable lex full-row sort → reslice to 65,536-
    row batches → digest). Scheduler `(TaskId → WorkerId)`
    assignment may differ across runs.
12. **Strict stage barriers.** Stage N+1 dispatch occurs only
    after all Stage N manifests are received and accepted;
    architecture-guarded that no consumer task starts before
    its producer stage completes.
13. **`ExchangeId` manifest coverage.** `ExchangeId` is
    allocated at plan time and appears in every shuffle
    partition address and partition manifest emitted on the
    wire. Architecture-guarded.
14. **Registry availability honored.** Workers marked `Lost`
    by the `WorkerRegistry` are not selected for new task
    attempts. A fixture that marks a worker `Lost` mid-query
    observes the coordinator routing the next `AttemptId` to
    a different worker.

### Soft gates

Threshold values are Phase 0 calibration targets. Recorded but
not blocking promotion.

- **Throughput scales with worker count.** Query wall-clock
  decreases when worker count rises from 2 → 4 → 8 on a
  fixed-cardinality GROUP BY workload. Target: ≥ 1.5×
  speedup per worker doubling.
- **Per-query counters recorded.** Task attempt count,
  accepted batch count, shuffle bytes published, shuffle
  bytes pulled, spill bytes used, rejected stale-message
  count, retry count, cancellation cleanup count. Surfaced
  via tracing / log spans.
- **Evidence matrix per fixture row.** Worker count, stage
  count, task count, exchange count, shuffle bytes published,
  shuffle bytes pulled, retry count, stale-output rejection
  count, cancellation cleanup count, result digest.
- **Failure-fixture reusability.** The SIGKILL fixture harness
  is parameterizable enough that ADR 004+ (distributed joins)
  can reuse it without rewriting.

## Resolved questions

Three iteration-resolved questions distilled into the V1
contract.

### Q1 — Bench evidence shape

ADR 002's matrix was bytes-fetched + speedup per query shape.
ADR 003's matrix combines:

- SIGKILL keystone correctness (hard gate #4).
- Throughput vs worker count (soft gate).
- Shuffle wall-clock at varying selectivity.
- Backpressure-under-load throughput stability.
- Stale-attempt rejection (hard gate #5) — at least one
  fixture.
- Cross-replica determinism digest pairs (hard gate #11).
- Per-row evidence matrix (soft gate).

Concrete fixture geometry pinned in the impl plan.

### Q2 — Cross-replica determinism gate shape

**Output parity after canonical normalization; worker
assignment may vary.** Two independent multi-worker runs of
the same query (one with a SIGKILL mid-shuffle) produce
byte-identical Arrow IPC output after the ADR §13 canonical
normalization: the pinned pipeline **concat → stable lex
full-row sort → reslice (65,536-row batches) → digest**.
The sort runs *before* reslice — reslicing first would only
canonicalize row order within each 65,536-row block, leaving
cross-block order non-deterministic. ADR 002's concat-then-
reslice-only normalization is insufficient under distributed
hash-aggregation + variable worker assignment + map
iteration nondeterminism. The `(TaskId → WorkerId)`
assignment may differ across runs.

### Q3 — `ExchangeId` allocation

Coordinator-assigned at plan-time, one per shuffle boundary in
the bound `ExecutionPlan`. `ExchangeId`s are stable across
attempts of the same query (idempotence rule 4 deterministic
partition assignment depends on this).

## Deferred (V1.x or later)

- **Distributed joins** (broadcast vs hash-partitioned, bloom
  pushdown, skew handling). Depends on ADR 003 shuffle +
  ADR 002 grouped aggregate. ADR 004+.
- **Multi-tenant resource governance** (per-tenant budgets,
  token-bucket admission, scheduler-slot allocation). Depends
  on ADR 003 query lifecycle. ADR 005+.
- **Pipelined cross-stage execution.** V1.x with a distinct
  failure-recovery story.
- **Locality-aware fragment routing.** V1.x; requires new
  Wendelon public-API surface.
- **WeakControlRing.** HA coordinators / cluster heartbeats
  via in-memory Raft. Wire envelope already carries lease /
  capability metadata for clean future swap. Brainstorm 002
  territory.
- **Replicated / disaggregated shuffle service.** V2; V1's
  per-worker partition store is the substrate.
- **RDMA / RoCEv2 / SIW transport.** QUIC preferred through
  V1; RDMA seam ADR is V1.x.
- **Operator-internal streaming end-to-end.** Stream-shaped
  task output is required by ADR 003 (budget envelope cap);
  operator-internal streaming is V1.x executor work.
- **Higher retry counts.** `max_attempts = 2` (one retry) is
  V1; future ADR raises with evidence.
- **Direct P2P shuffle as fast path.** V2 optimization over
  the V1 pull-addressed contract.

## Rejected alternatives

- **P2P-push as V1 default shuffle.** Fast on happy path,
  fragile to receiver death; retry semantics get knotted with
  producer state.
- **New `PhysicalFragment` IR level below `ExecutionPlan`.**
  Adds an IR transformation pass for no semantic benefit.
- **Pure spill-to-pull as V1 default.** Disk-write + decrypt
  latency on every shuffle including in-memory-sized queries.
- **Transport-native flow control alone as backpressure
  contract.** Wire-level only; doesn't bound memory or
  shuffle bytes.
- **RDMA / RoCEv2 / SIW transport in V1.** M9 seeds the
  transport-tier-negotiation shape; V1 keeps the contract
  transport-agnostic.
- **WeakControlRing implementation in V1.** Brainstorm 002
  territory. The `WorkerRegistry` trait carries lease /
  capability fields so the future swap is wire-break-free.
- **Pipelined cross-stage execution in V1.** Adds failure-
  recovery complexity without a concrete contract.
- **Locality-aware fragment routing in V1.** Would require
  new Wendelon public-API surface; conflates ADR 003's
  runtime contract with storage coordination.

## What Not To Do

- **Do not dispatch `LogicalPlan` to workers.** Coordinator
  binds; workers receive `TaskSpec` wrapping `ExecutionPlan`
  chunks.
- **Do not let workers rebind plans against their own
  catalog / planner state.** Binding lives in the coordinator
  under the pinned fingerprint.
- **Do not introduce a new IR level below `ExecutionPlan`.**
  `TaskSpec` wraps; it doesn't transform.
- **Do not initiate P2P push from producer to consumer.**
  Consumers always pull via `PullPartition`.
- **Do not skip `AttemptId` on partition addressing.** Stale-
  attempt rejection depends on it.
- **Do not stringify wire errors.** `TaskFailed` carries
  `TaskFailureKind`; partition store carries
  `PartitionMissingReason` / `RuntimeError`.
- **Do not conflate task-lifecycle failures with partition-
  store wire errors.** `TaskFailureKind` is 7 variants;
  `PartitionMissing` is a wire response, not a task-failure
  variant.
- **Do not use `tokio::time::sleep` for backpressure** in the
  production worker path. The per-query budget envelope is
  the contract; sleep mocks fail architecture guard.
- **Do not start Stage N+1 tasks before Stage N manifests
  are accepted.** Strict barrier; architecture-guarded.
- **Do not require stable worker assignment for
  determinism.** Output parity is the contract; assignment
  may vary.
- **Do not query Wendelon for ring placement.** V1 is
  location-agnostic. Locality is V1.x.
- **Do not extract `wendelon-net` / `wendelon-client` /
  `wendelon-types` into shared crates.** ADR 001 / 002
  import allowlist holds; extraction is V1.x with
  consumer-pressure evidence.
- **Do not bundle distributed joins, WeakControlRing, or
  tenant governance into ADR 003.** Each has its own
  contract surface; bundling defers real decisions.
- **Do not commit to a wire-format versioning policy for
  V1.x compat.** V1 rejects incompatible protocol versions
  outright; rolling upgrades are V1.x territory.
- **Do not raise `max_attempts` above 2 without an ADR.**
  V1 single retry; higher retry counts buy resilience at
  observable-determinism cost.
- **Do not let `MorselId` cross the wire.** It is worker-
  local scheduling detail; architecture-guarded out of
  distributed identity.

## Council decisions

Compact archival summary. Iteration history lives in git on
`plans/wip/`.

1. **ADR 003 = Distributed Runtime and Data Movement
   Contract.** All three voices converged topics iter-2.
2. **Dispatch `ExecutionPlan` chunks (`TaskSpec`), not new
   IR level.** Claude topics iter-1 framing; Codex ratified
   iter-2; Gemini conceded iter-2.
3. **Pull-addressed partition store with memory + spill
   backends.** Codex topics iter-1; adopted by all three
   iter-2.
4. **Per-query budget envelope as backpressure contract.**
   Codex iter-1; Claude / Gemini conceded iter-2.
5. **Nine-message task lifecycle + three-message data-
   movement protocol** (twelve message kinds across two
   surfaces). Codex iter-1 lifecycle; iter-3 surface split
   explicit.
6. **Seven typed task-failure variants** in
   `TaskFailureKind`; partition-store wire has its own
   error surface (`PartitionMissingReason` +
   `RuntimeError::PartitionStoreMismatch`). Codex iter-2
   taxonomy correction; Claude iter-3 absorbed.
7. **Identity newtype catalog (10 newtypes).** 7 identity-
   tuple + 3 wire-level (`LeaseEpoch`, `FrameByteLen`,
   `StreamSeq`). `MorselId` resolved as worker-local.
8. **`TaskIdentity` sub-struct in `TaskSpec`** with snake-
   cased `_id`-suffixed fields. Codex iter-2 / iter-3
   grouping; Claude iter-4 adopted.
9. **Four-rule idempotence contract.** Codex iter-1 Topic 2
   adopted verbatim.
10. **Coordinator / worker ownership separation.** Codex
    iter-1 Topic 8 adopted.
11. **Static `WorkerRegistry` with WeakControlRing design
    hooks.** All three voices iter-1.
12. **Versioned wire envelope.** Codex iter-1 Topic 7 adopted.
13. **Strict stage barriers V1.** All three voices iter-2.
14. **Location-agnostic V1; locality V1.x.** All three voices
    iter-2.
15. **SIGKILL keystone test.** All three voices.
16. **Cross-replica determinism: output parity after
    normalization; worker assignment may vary.** All three
    voices iter-2.
17. **Single retry default (`max_attempts = 2`).** Resolved
    Q5.
18. **`ExchangeId` coordinator-assigned at plan-time, one per
    shuffle boundary.** Resolved Q7.
19. **Transport-agnostic V1 contract.** QUIC preferred,
    framed-OS-socket acceptable. Codex iter-1 framing.
20. **Eight new architecture guards G8-G15** supplementing
    ADR 002's G1-G7. Exact predicate text pinned in Phase 0
    of the impl plan.
21. **Out of scope confirmed:** distributed joins, multi-
    tenant governance, WeakControlRing implementation,
    replicated shuffle service, pipelined execution, locality
    routing, RDMA, schema registry beyond ADR 002 pins,
    advanced optimizer work, full operator-internal streaming.
