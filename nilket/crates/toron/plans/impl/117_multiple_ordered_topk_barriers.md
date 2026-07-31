# Impl Plan — ADR 117: Multiple Ordered Top-K Barriers

**Migration for** `plans/adr/117_multiple_ordered_topk_barriers.md`. **Scope:** `distributed/fragment.rs` only
(threaded id allocator + explicit terminal-output parameter + recursive `global_sort_barrier` splitter + base
fragmenters made allocator-/terminal-output-aware) + tests + guards. **Non-scope:** no IR/executor/PBC/Wendelon
change; no merge-sort; no top-k pushdown; no barriers-inside-join-inputs; `Take` untouched.

## Pre-flight anchors

| Concern | Anchor |
|---|---|
| Id types (`u32` wrappers) | `StageId(u32)` / `ExchangeId(u32)` (`distributed/identity.rs:32/119`) |
| Cap to relax | `global_sort_barrier_count(&plan) > 1 → MultipleGlobalSortBarriers` (`fragment.rs:118`) |
| Named primitive + tail (reuse) | `global_sort_barrier` + `UnaryPlanTail` (115) |
| Output binding (the hinge) | `TaskOutputBinding::{FinalResult, ShuffleProducer { exchange_id, partition_count, partition_key }}` |
| Fixed-id sites (→ allocator) | `:146-148` (==1), `:247-250`/`:388-413` (joins), `:794-798` (sort-over-agg), `:527` (single-task) |
| Per-barrier stage builder (generalize) | `terminal_sort_stage` (`:889`), `fragment_terminal_sort_over_grouped_aggregate` (`:775`) |
| Scheduler (ready, untouched) | `StagePlan { stage_id, upstream_stages, tasks }` (`distributed/scheduler.rs`) |

## Phase 1 — Id allocator + terminal-output carrier

`FragmentIdAllocator { next_stage: u32, next_exchange: u32 }` with `alloc_stage()`/`alloc_exchange()`.
`enum FragmentTerminalOutput { FinalResult, ShuffleProducer { exchange_id: ExchangeId, partition_count: u32,
partition_key: BoundExprProgram } }`. Thread `&mut FragmentIdAllocator` + a `FragmentTerminalOutput` through all
fragmentation. Replace **every** `StageId::new(n)`/`ExchangeId::new(n)` literal on production paths with allocator
calls (intentionally-fixed identity literals like `TaskId::new(0)` are exempt).

## Phase 2 — Base fragmenters: allocator- + terminal-output-aware

Refactor each base to `(plan, terminal_output, &mut alloc, config) -> (Vec<StagePlan>, StageId /* terminal */)`:
- no-exchange / single-task; the `==1` producer+consumer split; `fragment_terminal_sort_over_grouped_aggregate`;
  `fragment_join_plan` / `fragment_hash_join_plan` / `fragment_broadcast_join_plan`.
- **Honor `terminal_output` (the hinge):** if `FinalResult`, the terminal task emits `FinalResult`; if
  `ShuffleProducer{..}`, the terminal stage **produces partition-shaped output** — wrap its terminal plan in a
  matching `ExecutionPlan::Exchange` unless that stage is already the exchange producer for that exact output, and
  set `TaskOutputBinding::ShuffleProducer`.

## Phase 3 — Recursive splitter (`fragment_plan` core)

```text
fn fragment(plan, terminal_output, &mut alloc, config) -> (Vec<StagePlan>, StageId):
  if let Some(barrier) = global_sort_barrier(&plan):
      let gather_exchange = alloc.alloc_exchange()
      // the barrier input must PRODUCE the single-partition shuffle the sort stage consumes
      let (mut stages, input_terminal) =
          fragment(barrier.input, ShuffleProducer{exchange_id: gather_exchange, partition_count:1, key:[]}, alloc, config)
      let sort_stage_id = alloc.alloc_stage()
      stages.push(barrier_stage(barrier, gather_exchange, input_terminal, sort_stage_id, terminal_output, alloc, config))
      (stages, sort_stage_id)
  else:
      fragment_base(plan, terminal_output, alloc, config)   // Phase 2
```
- `barrier_stage` generalizes `terminal_sort_stage`: a consumer reading the `gather_exchange` shuffle, running
  `execute_sort`, applying the `UnaryPlanTail` (115), `upstream_stages = [input_terminal]`, and a
  `ShuffleConsumer { exchange_id: gather_exchange, producer_stage_id: input_terminal, partition_id: 0, … }`.
  It emits `terminal_output` (so a non-final barrier itself produces the next shuffle — D3 composition).
- Remove the `global_sort_barrier_count > 1` reject; keep `global_sort_barrier_count` as a helper/guard. Keep the
  DDL/DML not-fragmentable guards. `contains_join` becomes a `fragment_base` concern (a barrier may sit above a
  fragmented join; a barrier *inside* a join input is not fragmented → pinned reject).
- Top-level call: `fragment(plan, FinalResult, &mut alloc, config)`.

## Phase 4 — Tests + guards

Behavior (per ADR §gates): `rune_top_k_of_top_k_fragments_and_executes` (data proving the inner barrier matters),
`three_global_sort_barriers_allocate_unique_stage_and_exchange_ids` (only final stage = `FinalResult`),
`grouped_aggregate_then_two_ordered_barriers_fragments`,
`barrier_above_join_composes_but_inside_join_input_rejects_with_pin`, `ordered_offset_applies_per_barrier`,
regressions (`single_barrier_over_scan`, `single_barrier_over_aggregate`, `ordered_offset`,
`unordered_take_not_a_barrier`). Assert stage-DAG shape + `upstream_stages` + id uniqueness + terminal-output
shape, not row identity where unordered.
Guards: retarget `adr115_multiple_global_sorts_reject_with_pin` → `adr117_multiple_global_sorts_split_recursively`;
strengthen `adr115_global_sort_barrier_is_a_named_primitive` (recursive consumption);
`adr117_terminal_output_is_explicit` (carrier + shuffle-wraps-Exchange + only-final-FinalResult);
`adr117_fragment_ids_are_allocated_not_fixed`; `adr117_no_barrier_inside_join_inputs`; `adr117_take_is_not_a_sort_barrier`;
`adr117_no_merge_sort_or_topk_pushdown`; `adr117_no_sql_barrier_flattening_rewrite`.

## Phase 5 — Verification

```sh
cargo fmt
cargo test -p toron --test architecture_test adr117
cargo test -p toron --test architecture_test adr115
cargo test -p toron --test architecture_test adr116
cargo test -p toron --test sort_ordering_test
cargo test -p toron --test rune_relation_handles_test
cargo test -p toron --test group_by_test
cargo test -p toron --test distributed_process_test
cargo test -p toron
cargo check -p toron --all-targets   # distributed-planner cut → non-default targets (the 116 lesson)
git diff --check
```

## Acceptance

Multiple authored ordered top-k barriers fragment into a correct multi-stage DAG via a recursive `global_sort_barrier`
splitter; the terminal output is an explicit parameter (shuffle terminals produce partition-shaped `Exchange`
output, only the final stage emits `FinalResult`); stage/exchange ids are allocated monotonically (no fixed
literals on production paths); each `.sort().limit()` boundary is preserved (Rune D9; top-k-of-a-top-k correct);
barriers compose above scan/aggregate/join bases but reject inside join inputs; 115/116 single-barrier / ordered-
OFFSET / unordered-Take behavior unchanged; reuse-only; the retargeted 115 guard + `adr117_*` guards hold; targeted
+ `--all-targets` + Toron suite green.
