# Impl Plan — ADR 119: Pipelined Cross-Stage Execution (V1, single-exchange)

**Migration for** `plans/adr/119_pipelined_cross_stage_execution.md`. **Scope:** Toron only — scheduler
in-flight frontier + expected-dependency readiness, `task.rs` `ShufflePartitionRef::{Sealed,Live}`
widening, partition store open/append/seal/cancel lifecycle, worker streamable producer path, and worker
streamable consumer path. **Non-scope:** Wendelon protocol; SQL/Rune/IR semantics; client-facing
`FinalResult` streaming; multi-upstream pipelining.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Inline-serial dispatch (the temporal barrier) | `dispatch_loop` `dispatcher.dispatch(...).await` (`scheduler.rs:275-278`) |
| Per-stage barrier (the gate to bifurcate) | `stage_barrier_open` (`:499-519`); call site `:205` |
| Current dependency map (post-manifest, not enough for readiness) | `task_dependencies: HashMap<TaskKey, Vec<PartitionAddress>>` (`:108`); populated at `:454`; helper `task_dependencies(spec)` (`:742`) reads already materialized refs |
| Post-completion publish (the seal barrier) | `accepted_manifests.insert(key, manifest)` in `accept_dispatch_completion` (`:447`) |
| Consumer resolves refs from sealed manifests | `materialize_task_spec` (`:521-585`) reads `accepted_manifests` (`:553-557`) |
| Producer vector barrier | `ExecutionPlan::Exchange` calls `bound_exchange`, which first `execute_bound`s its child into `Vec<RecordBatch>` |
| Consumer vector barrier | `server/mod.rs::pull_shuffle_inputs` returns `Vec<RecordBatch>`; `ScanOverride` stores `batches: Vec<RecordBatch>`; `ExecutionPlan::Scan` materializes `ScanStream` in `execute_bound` |
| Worker shuffle producer | `crates/toron/src/server/mod.rs::execute_task_local_with_telemetry` + `publish_task_output` |
| Existing partition store | `crates/toron/src/distributed/partition_store.rs` (sealed-only publish/pull) |
| 118 cost-bound payoff target | `cancelled_consumer_stops_before_all_row_groups_scanned` (projected path) — the gate this ADR's downstream-cancel test feeds into |
| 117 multi-upstream invariant (must stay exact) | `stage_barrier_dispatches_consumer_after_upstream_acceptance` + `stage_barrier_waits_for_both_upstreams` + `consumer_stage_reports_no_progress_until_barrier_opens` |
| Cancellation fanout | `ActiveQueryRegistry`, `DownstreamCancellationContext`, 059/060 token chain (118 already threads it into Wendelon) |

## Phase 0 — Write the failing pipelining gate first

Add `single_exchange_consumer_starts_before_producer_task_complete` to scheduler/worker integration tests:
producer task opens one live partition, emits one batch, blocks before `TaskComplete`; consumer task with
one live ref dispatches and pulls the first batch *before* producer's terminal state. **Must fail on
`main`.** If it passes without live partition reads, the test is not proving pipelining — revise it. This
gate stays RED until Phases 1–6 land.

Add three companion red gates before implementation:

- `streaming_exchange_producer_appends_before_execute_bound_would_return`
- `live_consumer_does_not_materialize_all_shuffle_input_before_execution`
- `consumer_live_refs_cover_all_expected_producers`

## Phase 1 — In-flight scheduler frontier (D1)

Refactor `dispatch_loop` to hold an in-flight `FuturesUnordered<TaskAttemptFuture>` (or `JoinSet`). Ready
tasks are spawned onto the frontier; completions feed `accept_dispatch_completion` (existing path); retries
schedule a new attempt onto the frontier (don't loop inside one task). Bounded concurrency:
worker-capacity-driven (the existing `WorkerRegistry::select_for_attempt` capacity is the natural cap),
with a per-query maximum surfaced as `QueryBudget::max_in_flight_tasks: Option<usize>` (default `None` =
unbounded by 119) — a back-pressure safety net. Preserve `progressed`/`blocked_stages`/`NoProgress`
deadlock detection by extending its "blocked" set to include single-upstream stages whose tasks aren't
ready.

**Guardrail:** if Phase 1 lands and Phase 0 still passes, the implementation has somehow re-entered the
inline-await shape — STOP. The honesty-check gate `barrier_only_revert_would_break_the_pipelining_gate`
exists for this.

## Phase 2 — Expected-dependency readiness (D2)

Add a scheduler-side expected-dependency surface built from `StagePlan` before dispatch. The current
`task_dependencies(spec)` helper is post-manifest because it reads `producer_partitions` after
`materialize_task_spec` fills them; it cannot be the only pre-dispatch readiness source.

One acceptable shape:

```rust
struct ExpectedShuffleDependency {
    exchange_id: ExchangeId,
    producer_stage_id: StageId,
    consumer_partition_id: PartitionId,
    producer_task_keys: Vec<TaskKey>,
}
```

New helper `fn task_ready_for_dispatch(&self, key: TaskKey) -> bool` returns true when every expected
producer task for the consumer input resolves to either a sealed manifest (existing path) or a running live
attempt (producer in `AttemptState::Running` per D10, Phase 3). For single-upstream stages, the frontier's
admission gate uses this. For multi-upstream stages, `stage_barrier_open` continues to gate dispatch
exactly — its `upstream_stages.iter().all(...) == LiveAccepted` predicate is the 117 invariant. Preserve
the existing `task_dependencies` map only where it still helps accepted-manifest invalidation/drain logic.

## Phase 3 — `ShufflePartitionRef::{Sealed,Live}` widening (D3)

In `crates/toron/src/distributed/task.rs`:

```rust
pub enum ShufflePartitionRef {
    Sealed(ProducerPartitionRef),
    Live(LiveProducerPartitionRef),
}

pub struct LiveProducerPartitionRef {
    pub address: PartitionAddress,
    pub worker_id: WorkerId,
    pub worker_endpoint: WorkerEndpoint,
    // NO digest, NO size_bytes — finals materialize at seal
}
```

Widen `TaskInputBinding::ShuffleConsumer.producer_partitions: Vec<ProducerPartitionRef>` to
`Vec<ShufflePartitionRef>`. `materialize_task_spec` builds `Live` variants for producers in `Running` state
(deterministic attempt-scoped addresses from the producer spec); falls back to `Sealed` once the producer is
accepted, or unconditionally `Sealed` when the producer is non-streamable (Phase 5 predicate returns false).
The materializer must reject partial sets: if a consumer partition expects N producer tasks, the generated
refs must cover N producer attempts, sealed or live. Same-version Toron wire only; no cross-version
compatibility promise.

## Phase 4 — Partition store live lifecycle (D4)

In `crates/toron/src/distributed/partition_store.rs`, add beside the existing sealed `publish`/`pull`:

```rust
fn open_live(&self, addr: PartitionAddress) -> LiveProducerHandle;
async fn append_live(&self, addr: PartitionAddress, chunk: Bytes) -> Result<(), StoreError>;
async fn seal_live(&self, addr: PartitionAddress, digest: Digest, size_bytes: u64, row_count: u64)
    -> Result<SealedPartitionManifest, StoreError>;
async fn cancel_live(&self, addr: PartitionAddress, reason: CancelReason) -> Result<(), StoreError>;
async fn pull_live(&self, addr: PartitionAddress) -> LivePartitionStream;
    // blocks until open/first chunk / seal / cancel|fail; NO independent timeout in V1
```

Existing `publish(manifest, payloads)` remains valid (backs fallback path); implement either directly or as
`open_live` → `append_live`*N → `seal_live`. `pull_live` may arrive before `open_live`; it waits for
open/append/seal/cancel/failure instead of returning an immediate not-found success path. Appends reserve
shuffle budget incrementally and buffering is bounded. Sealed regressions stay behavior-identical.

## Phase 5 — Worker streamable predicate + streaming producer (D5)

In the worker (`crates/toron/src/server/mod.rs`):

```rust
fn is_streamable_exchange_producer(spec: &TaskSpec) -> bool;
```

Returns true iff `spec.output` is `ShuffleProducer` AND the wrapping `ExecutionPlan::Exchange.child` chain
is composed of `Scan` / `Filter` / `Project` / `Take` / identity wrappers only (recursive); any
`Sort`/`Aggregate`/`Join`/blocking shape ⇒ false. Worker exchange execution:

```rust
if is_streamable_exchange_producer(spec) {
    // live path:
    //   - open_live each partition address before execution
    //   - drive the exchange child on input batches, evaluating exchange partition key per batch
    //   - append_live partition chunks as produced
    //   - seal_live at end → emit TaskComplete with sealed manifest
    //     (existing acceptance + drain semantics preserved)
} else {
    // sealed path: existing publish-at-complete behavior, unchanged
}
```

The live producer path must not call `Executor::execute_bound(&spec.plan, ...)` on the terminal
`ExecutionPlan::Exchange`, and must not route through `bound_exchange`. For `Scan`, use the storage
`ScanStream` directly. For `Filter`, `Project`, and `Take`, add batch-by-batch helpers instead of calling
their current vector-returning `execute_bound` implementations. `Take` must stop pulling and trigger
cancellation once its limit is satisfied. On the projected Wendelon path, this is the 118 cost-bound chain's
source end.

## Phase 6 — Consumer live pull + streaming execution (D6)

`shuffle_input_overrides` (worker-side) dispatches on `ShufflePartitionRef::{Sealed,Live}`. For `Live`, call
`pull_live(addr)`: blocks until first chunk, seal, or cancel/fail; yields `RecordBatch`es preserving
producer batch boundaries. Do not collect live refs through `pull_shuffle_inputs -> Vec<RecordBatch> ->
ScanOverride { batches }`.

Add a live consumer execution path that does not call the normal vector-materializing
`Executor::execute_bound` on a plan whose shuffle scan is live. For V1, support the same batch-by-batch
suffix as the producer path: `Scan`, `Filter`, `Project`, `Take`, and identity wrappers. If the consumer
contains a blocking shape (`Sort`, `Aggregate`, `Join`, or unknown), fall back to the sealed/materialized
path explicitly. This is required because the current `ExecutionPlan::Scan` branch materializes a
`ScanStream` into `Vec<RecordBatch>` before parent operators run.

Cancellation: producer failure or transport break surfaces as task failure — not a successful partial result
(118-D8 honesty preserved across the exchange boundary).

If a new pull-frame wire kind is needed, keep it Toron-worker-protocol local (no Wendelon-wire change, no
SQL-frontend change). Pin the localization via `adr119_no_wendelon_or_frontend_change`.

## Phase 7 — Cancellation chain → 118 payoff (D7)

- Consumer task that finishes early drops its `LivePartitionStream` handle → triggers `cancel_live` on the
  producer's addresses.
- `ActiveQueryRegistry` cancellation fanout cancels still-running producer tasks via the existing 059/060
  token chain.
- Producer worker's cancellation token reaches the executor's `Scan` → (projected path) Wendelon's
  `TABLE_QUERY_REQ` stream → Wendelon stops scanning before all row groups read.
- Partition store wakes blocked readers with the cancel reason and releases address reservations.

Add telemetry only if existing cancellation metrics cannot classify live-shuffle cancellation; avoid
per-query or per-task labels.

## Phase 8 — Behavior tests + architecture guards

**Behavior** (in `distributed::scheduler::tests` + integration suites):

- `single_exchange_consumer_starts_before_producer_task_complete` (Phase 0; pipelining proof)
- `streaming_exchange_producer_appends_before_execute_bound_would_return` (producer vector-barrier proof)
- `live_consumer_does_not_materialize_all_shuffle_input_before_execution` (consumer vector-barrier proof)
- `consumer_live_refs_cover_all_expected_producers` (readiness completeness)
- `live_pull_before_open_waits_for_open_append_or_cancel` (D4/D10)
- `downstream_take_cancels_live_shuffle_and_stops_upstream_scan` (118 payoff)
- `non_streamable_exchange_falls_back_to_sealed_manifest` (D8)
- `multi_upstream_stage_keeps_stage_barrier` (D9 / 117 invariant)
- `live_partition_failure_fails_query_without_partial_success` (D6 honesty)
- `hash_shuffle_full_fan_in_still_waits_for_all_producers` (D8 no-regression)
- `barrier_only_revert_would_break_the_pipelining_gate` (honesty check)
- Regressions: scheduler retry / worker loss invalidation / 117 multi-upstream / 118 projected cost-bound — unchanged.

**Guards** (in `crates/toron/tests/architecture/distributed_execution/scheduler_dispatch.rs` +
`crates/toron/tests/architecture/predicate_pushdown.rs` as appropriate):

- `adr119_dispatch_loop_has_in_flight_frontier`
- `adr119_expected_dependencies_are_pre_manifest`
- `adr119_streamable_plan_predicate_is_explicit`
- `adr119_live_refs_are_explicit_not_fake_sealed_refs`
- `adr119_partition_store_has_open_append_seal_cancel_lifecycle`
- `adr119_live_exchange_path_does_not_call_bound_exchange`
- `adr119_live_shuffle_input_is_stream_backed_not_vec_override`
- `adr119_live_consumer_path_does_not_call_execute_bound_on_live_scan`
- `adr119_multi_upstream_barrier_untouched`
- `adr119_non_streamable_fallback_is_explicit`
- `adr119_no_wendelon_or_frontend_change`
- `adr119_consumer_readiness_uses_running_not_advertisement`

## Phase 9 — Verification

```sh
cargo fmt
cargo test -p toron distributed::scheduler::tests
cargo test -p toron distributed::partition_store::tests
cargo test -p toron --test architecture_test adr117 adr118 adr119
cargo test -p toron --test distributed_process_test
cargo test -p toron --test wendelon_storage_execution_test
cargo test -p toron
cargo check -p toron --all-targets
git diff --check
```

## Acceptance

Phase 0's `single_exchange_consumer_starts_before_producer_task_complete` is green; producer and consumer
live paths bypass the current vector-materialization barriers; downstream-take cancels live upstream and
(through 118 projected path) bounds Wendelon's scan; non-streamable producers fall back to sealed manifests
explicitly; multi-upstream stages keep 117's barrier exact; all `adr119_*` guards + 115/116/117/118
regressions hold; full Toron suite + `--all-targets` green; no SQL/Rune/IR or Wendelon change. Migration:
`plans/adr/119_pipelined_cross_stage_execution.md`.
