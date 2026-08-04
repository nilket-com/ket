# ADR 119: Pipelined Cross-Stage Execution (V1, single-exchange)

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — Claude conceded iter-1 to
grounding; iter-2 adopted Codex's structural cut; iter-3 locked the live tensions. **Category:** Toron
interior — distributed execution substrate (scheduler + partition store + worker shuffle path).
**Predecessors:** 117 (recursive fragmenter; multi-upstream acceptance barrier — preserved), 118
(stream-shaped storage boundary; the downstream-cancel-stops-upstream-scan payoff that 119 finally surfaces
end-to-end). **Migration:** `plans/impl/0119_pipelined_cross_stage_execution.md`.

## Scale boundary

ADR 119 is the **consumer-side capstone** of the 115→118 substrate arc: a Toron consumer of a single
upstream exchange can begin reading the upstream's output *before* the producer task reaches terminal
`TaskComplete`, and an early consumer drop cancels the still-running producer — which through 118's
projected path stops Wendelon's table-query stream before all qualifying row groups are read. The cut is
intentionally **single-exchange**: multi-upstream stages (joins, multi-input compositions) keep 117's
acceptance-layer barrier untouched. Non-scope: SQL/Rune/IR semantics, Wendelon protocol, client-facing
final-result streaming, intra-row streaming. Non-streamable producer plans (`Sort`/`Aggregate`/`Join`)
**explicitly fall back** to the existing sealed-manifest publish — fallback is named, tested, and guarded;
it is not hidden under the "pipelined" claim.

## Doctrine — four barriers, not one

> A stage barrier is a correctness dependency; an in-flight scheduler frontier is an execution strategy; a
> live partition lifecycle plus streaming operator paths is what lets the strategy bite. ADR 119 needs all
> four.

Four concurrent obstacles block end-to-end pipelining today, and a cut named "pipelined cross-stage
execution" must clear all four:

1. **`stage_barrier_open`** (`scheduler.rs:499-519`) waits for the entire upstream stage's acceptance — a
   per-task readiness check is necessary here but in isolation does nothing.
2. **Inline-serial dispatch** (`scheduler.rs:275-278`) holds only one task in flight, so even with per-task
   readiness, the scheduler can't overlap producer and consumer execution.
3. **Post-completion publish** (`scheduler.rs:447`) gates the consumer's input on the producer's terminal
   manifest, so even with concurrent dispatch the consumer has nothing to read until the producer is done.
4. **Vector-shaped worker execution** materializes both sides of the exchange today:
   `ExecutionPlan::Exchange` calls `bound_exchange`, which first `execute_bound`s its child into
   `Vec<RecordBatch>`, while `pull_shuffle_inputs` and `ScanOverride { batches }` pull shuffle input into a
   `Vec<RecordBatch>` before the consumer plan runs.

Each obstacle alone is sufficient to defeat pipelining; clearing three without the fourth still ships
barrier relaxation under a misnamed banner. V1 clears all four for the single-exchange + streamable
scan/filter/project/take subset, with an explicit fallback for everything else.

## Decisions (promotion-locked)

**D1 — In-flight scheduler frontier.** `dispatch_loop` becomes a bounded `FuturesUnordered`/`JoinSet`
frontier of in-flight task futures. Ready tasks are added to the frontier; completions update
`TaskStateStore`, `accepted_manifests`, runtime-filter fragments, retries, and final results through the
existing `accept_dispatch_completion` path. Retries schedule a new attempt onto the frontier instead of
looping inside one task. **Bounded concurrency is worker-capacity-driven** (the existing
`WorkerRegistry::select_for_attempt` capacity is the natural cap), with a per-query maximum surfaced as
`QueryBudget::max_in_flight_tasks: Option<usize>` (default `None` = unbounded by 119) — a back-pressure
safety net for runaway DAGs, not a tuning knob. The serial outer loop is the regression we must not roll
back to.

**D2 — Per-task readiness uses complete expected dependencies, not post-manifest `task_dependencies`.**
Source stages are ready as today. Multi-upstream stages keep `stage_barrier_open` exactly. A
single-upstream consumer task is **ready** only when every producer task expected to write that consumer
partition resolves to either a sealed manifest (existing path) or a live ref against a producer attempt in
`AttemptState::Running` (D3+D10). Current `task_dependencies(spec)` is populated from already materialized
`producer_partitions`, so it is post-manifest state and cannot be the only pre-dispatch readiness source.
Add an explicit scheduler-side expected-dependency surface derived from the `StagePlan`: producer stage id,
exchange id, consumer partition id, and the complete set of producer tasks that can write that partition.
Never dispatch with only the subset of producers that happen to be running.

**D3 — Live partition refs are explicit, not fake-sealed.** Widen the element type of
`TaskInputBinding::ShuffleConsumer.producer_partitions` from `ProducerPartitionRef` to
`enum ShufflePartitionRef { Sealed(ProducerPartitionRef), Live(LiveProducerPartitionRef) }`.
`LiveProducerPartitionRef` carries fields known before seal only: `PartitionAddress`, `WorkerId`,
`WorkerEndpoint`. No digest/size on `Live` — finals materialize at seal. The live address is attempt-scoped;
producer retry, worker loss, or cancellation invalidates dependent live readers instead of silently
retargeting them to a new attempt. Same-version Toron wire only; no cross-version compatibility promise
needed.

**D4 — Partition store gains an open/append/seal/cancel lifecycle.** Beside the existing publish-once API:
`open_live(addr)` reserves identity + creates a reader notification point; `append_live(addr, chunk)`
appends one Arrow IPC chunk and wakes readers; `seal_live(addr, digest, size_bytes, row_count)` marks EOF
and surfaces the sealed manifest; `cancel_live(addr | query)` wakes readers with a cancellation reason and
releases budget; `pull_live(addr)` blocks until the first chunk, the seal, or a cancel/fail. The existing
sealed `publish`/`pull` remains valid and backs the fallback path — sealed-only regressions stay
behavior-identical. **No independent `pull_live` timeout in V1**; blocked reads resolve through producer
append/seal/cancel/failure or ordinary query cancellation. Do not add a new wall-time budget field just for
this cut.

**D5 — Worker streamable-plan predicate is an explicit named code surface.** A new predicate
`is_streamable_exchange_producer(spec) -> bool` returns true iff the terminal output is
`TaskOutputBinding::ShuffleProducer` *and* the wrapping `ExecutionPlan::Exchange`'s child chain is composed
only of `Scan` / `Filter` / `Project` / `Take` (and identity wrappers proven safe by existing executor
logic). Anything containing `Sort`, `Aggregate`, `Join`, or any blocking shape returns false. The worker
takes the live path iff the predicate holds; otherwise it takes the sealed path. The predicate is a typed
function, not an inline `match` arm buried in the executor. The live producer path must bypass
`bound_exchange`/`execute_bound` on the terminal exchange and append partition chunks as child batches are
produced; otherwise it recreates the old producer-side vector barrier.

**D6 — Consumer live input is stream-backed through execution.** `shuffle_input_overrides` dispatches on
`ShufflePartitionRef::{Sealed,Live}`. A live pull yields `RecordBatch`es preserving producer batch
boundaries until seal, but the consumer must not then collect those batches into `ScanOverride { batches:
Vec<_> }` and call the normal `execute_bound` scan path. Add an explicit streamable-consumer path for the
scan/filter/project/take suffix that reads live shuffle batches and runs downstream operators
batch-by-batch. Blocking consumer plans fall back explicitly to the sealed/materialized path. Transport
failure or producer cancellation surfaces as a task failure — never a successful partial result (118-D8's
terminal-error-after-data contract continues to hold across the exchange boundary).

**D7 — Cancellation closes upstream live exchanges → 118 payoff chain.** When a downstream consumer
finishes early or the query is cancelled: live partition readers close (their `LivePartitionStream` drops
→ `cancel_live`); the active-query cancellation fanout cancels still-running upstream producer tasks; the
producer worker's cancellation token reaches the executor's `Scan`, which on the projected path reaches
Wendelon and stops the table-query stream before all row groups are read. This is the load-bearing
observable that links 119 to the 118 cost bound.

**D8 — Non-streamable and full-fan-in cases degenerate explicitly.** If the producer or consumer plan is not
streamable (D5/D6) *or* a consumer partition depends on every producer (hash-shuffle full fan-in), V1 may
behave like the existing sealed-manifest path. This is the **declared** fallback, tested + guarded by name.
Fallback is not hidden under the "pipelined" banner — barrier relaxation alone (without the live partition
lifecycle and streaming execution paths) does not earn the cut's name.

**D9 — Multi-upstream stages keep 117's acceptance-layer barrier.** Joins / multi-input compositions
(`upstream_stages.len() > 1`) continue to use `stage_barrier_open` with its `LiveAccepted` predicate
unchanged. Live multi-input pipelining is a deliberately-named follow-on, not 119.

**D10 — Consumer readiness uses `AttemptState::Running`, not a separate advertisement.** Live partition
addresses are deterministically derived from the producer task spec; the scheduler knows them at dispatch
time. A consumer is ready when its producer is in `Running` (or already accepted) — no separate
"address advertised" wire frame. The partition store's `pull_live(addr)` is the natural sync point: it
blocks until the producer's first `append_live` or a terminal cancel/failure.

## Behavior gates

- **`single_exchange_consumer_starts_before_producer_task_complete`** (the load-bearing pipelining proof) —
  a producer opens a live partition, emits one batch, blocks before `TaskComplete`; the consumer dispatches
  and pulls the first batch *before* the producer's terminal state. Observable: consumer's `Running`
  precedes producer's `LiveAccepted`.
- **`streaming_exchange_producer_appends_before_execute_bound_would_return`** — a streamable producer
  appends a partition chunk while its child scan is still open; proves the live producer path bypasses
  `bound_exchange`/`execute_bound` vector materialization.
- **`live_consumer_does_not_materialize_all_shuffle_input_before_execution`** — a live input with one chunk
  and an unsealed tail lets the consumer plan start after the first chunk; fails if the live path falls back
  through `ScanOverride { batches }` or `execute_bound` scan materialization.
- **`downstream_take_cancels_live_shuffle_and_stops_upstream_scan`** (the 118-payoff headline) — a bounded
  consumer reads enough rows, drops the live partition, cancellation propagates to the producer's Wendelon
  scan, and Wendelon stops before all qualifying row groups are read.
- **`consumer_live_refs_cover_all_expected_producers`** — the scheduler refuses to dispatch a consumer with
  only the running subset of expected producer refs.
- **`live_pull_before_open_waits_for_open_append_or_cancel`** — a live pull that arrives before
  `open_live` blocks and then resolves through open/append, seal, cancellation, or failure.
- **`non_streamable_exchange_falls_back_to_sealed_manifest`** (D8) — aggregate/sort/join producers still
  publish-at-complete; correctness preserved; the consumer sees a sealed ref, not a live one.
- **`multi_upstream_stage_keeps_stage_barrier`** (D9) — a two-upstream consumer cannot dispatch through any
  live ref; 117's barrier is exact.
- **`live_partition_failure_fails_query_without_partial_success`** (D6 / 118-D8 honesty) — producer failure
  mid-stream fails the consumer's query; emitted-so-far is not a successful partial result.
- **`hash_shuffle_full_fan_in_still_waits_for_all_producers`** (D8 no-regression) — when consumer deps
  reference every producer, dispatch waits for the last producer's acceptance.
- **`barrier_only_revert_would_break_the_pipelining_gate`** (honesty check) — removing the live partition
  path while keeping the per-task barrier would make the headline pipelining gate fail; pinned at the test
  layer.
- Regressions: full 115/116/117/118 suites unchanged.

## Architecture guards

- **`adr119_dispatch_loop_has_in_flight_frontier`** — `dispatch_loop` source contains `FuturesUnordered`
  or `JoinSet`; the inline-await pattern is not the sole execution shape.
- **`adr119_expected_dependencies_are_pre_manifest`** — readiness is not based only on
  `task_dependencies(spec)` over already-filled `producer_partitions`.
- **`adr119_streamable_plan_predicate_is_explicit`** — `is_streamable_exchange_producer` (or
  equivalently-named predicate) exists as a named fn; the worker's choice between live and sealed paths
  reads it (no inline `match` on plan shape buried in the executor).
- **`adr119_live_refs_are_explicit_not_fake_sealed_refs`** — `ShufflePartitionRef::Live` exists and carries
  no digest/size fields (lexical pin on the struct definition).
- **`adr119_partition_store_has_open_append_seal_cancel_lifecycle`** — partition store source contains
  `open_live` / `append_live` / `seal_live` / `cancel_live` / `pull_live` (or equivalent) named methods.
- **`adr119_live_exchange_path_does_not_call_bound_exchange`** — the live producer branch does not route
  terminal exchanges through `bound_exchange`.
- **`adr119_live_shuffle_input_is_stream_backed_not_vec_override`** — live refs do not flow through
  `ScanOverride { batches: Vec<RecordBatch> }`.
- **`adr119_live_consumer_path_does_not_call_execute_bound_on_live_scan`** — the live consumer branch does
  not call the normal vector-materializing `execute_bound` path for a live shuffle scan.
- **`adr119_multi_upstream_barrier_untouched`** — `stage_barrier_open` source still contains
  `upstream_stages.iter().all(...)` + `LiveAccepted`; still invoked for `upstream_stages.len() > 1`.
- **`adr119_non_streamable_fallback_is_explicit`** — streamability predicate is checked *before* choosing
  the live exchange path; the sealed fallback is reachable from the worker source.
- **`adr119_no_wendelon_or_frontend_change`** — IR/SQL/Rune/PBC + Wendelon source files contain no
  `live_partition` / `ShufflePartitionRef::Live` / live-pull markers (localization pin).
- **`adr119_consumer_readiness_uses_running_not_advertisement`** — no `live_advertisement` /
  `advertise_partition` wire frame; readiness reads `AttemptState::Running`.
- **Recipe:** `cargo test -p wendelon` (unchanged but must still pass) + `cargo test -p toron` +
  `cargo test -p toron --test architecture_test adr117 adr118 adr119` +
  `cargo check -p toron --all-targets`.

## Council history

iter-0: 3-of-3 on topic A. iter-1: Claude drafted a per-task `task_dependencies` barrier relaxation; Codex
grounded that this is necessary but not sufficient (verified inline-serial `dispatcher.dispatch().await` at
`scheduler.rs:275-278` + post-completion `accepted_manifests.insert` at `:447`). Claude **conceded iter-1
to grounding** — the structure-ossification self-check inverted because the smaller cut avoided opening the
partition store + worker protocol, exactly where the named behavior lives; the lesson sat as
[[feedback_cut_must_deliver_the_name]]. iter-2: Claude adopted Codex's D1-D9 structural cut and contributed
(a) the need for per-task readiness as the frontier's admission shape, (b)
`is_streamable_exchange_producer` as a typed named predicate (D5), (c) consumer readiness uses
producer's `Running` state + deterministic addresses, not a separate advertisement frame (D10), (d) the
`barrier_only_revert_would_break_the_pipelining_gate` honesty-check gate that pins the discipline at the
test layer. Codex's final pass tightened readiness to a complete expected-dependency surface because today's
`task_dependencies(spec)` is post-manifest, and tightened the worker side to bypass both producer and
consumer vector barriers. iter-3 locked the live tensions: worker-capacity-driven frontier concurrency with
a `QueryBudget::max_in_flight_tasks` per-query safety cap (default `None` = unbounded by 119); no
independent `pull_live` timeout in V1.

## What to do next

Multi-upstream live pipelining (relaxes 117 multi-upstream barrier for the streamable subset);
client-facing `FinalResult` streaming to the SQL caller; whole-record table-query cost-streaming follow-on
from ADR 118 ([[adr_118_cost_bound_scope_projected_path]]); streamable-plan coverage extension
(bounded-memory streaming shapes for `Aggregate`/`Sort`); joins frontier; logical-type lifecycle
(`DROP`/`ALTER LOGICAL TYPE`); declared-additive types.

## Promotion criteria

A single-exchange consumer demonstrably reads producer output before the producer reaches `TaskComplete`
(behavior-gate evidence); a downstream-bounded consumer cancels live upstream work and (through ADR 118
projected path) stops Wendelon's table-query stream before all row groups are read; non-streamable producers
fall back to sealed manifests explicitly; multi-upstream stages keep 117's barrier exact; no SQL/Rune/IR or
Wendelon change; the `adr119_*` guards plus all 115/116/117/118 regressions hold; full
`cargo test -p toron` + `cargo test -p toron --test architecture_test adr117 adr118 adr119` +
`cargo check -p toron --all-targets` green.
Migration: `plans/impl/0119_pipelined_cross_stage_execution.md`.
