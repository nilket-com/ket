# ADR 117: Multiple Ordered Top-K Barriers

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini).** **Toron distributed fragmenter only —
no IR/executor/PBC/Wendelon change.** **Predecessors:** 010/015 (terminal sort), 115 (the `global_sort_barrier`
named primitive + the single-barrier `MultipleGlobalSortBarriers` cap this relaxes + the D9 authored-Rune-boundary
doctrine), 116 (unordered `Take` — not a sort barrier, untouched). **Migration:**
`plans/impl/117_multiple_ordered_topk_barriers.md`.

## Doctrine — relax the cap by recursing the primitive, not by special-casing

115 built `global_sort_barrier` as a *named, reusable* primitive and capped plans at one barrier with the explicit
promise that lifting the cap = a recursive splitter, not a new bespoke shape. 117 cashes that in: generalize
`fragment_plan` from a flat single-boundary dispatcher into a **recursive stage-splitter**. The scheduler already
runs multi-stage DAGs (`StagePlan { upstream_stages }`; aggregate-sort and join plans already use ≥3 stages), so
this is **fragmenter-local** — reuse `Sort`/`ExecutionPlan::Sort`/`execute_sort` + the empty-key single-partition
gather `Exchange`. **No merge-sort, no top-k pushdown, no new IR/executor/Wendelon.**

**Rune is the reason (115 D9):** `df.sort([desc(revenue)]).limit(100).sort([asc(created_at)]).limit(10)` = "the 10
oldest among the top 100 by revenue." Flattening to one sort, commuting predicates across a barrier, or treating an
inner barrier as advisory **changes the row set**. Each authored `.sort().limit()` stays a concrete gather+sort
stage. SQL gets no new doctrine — if its lowering already produces the same IR, the fragmenter may execute it; **no
SQL-specific flattening/rewrites.**

## The two load-bearing seams (grounded)

1. **Fixed ids everywhere → a threaded allocator (Claude).** Every fragmenter path hard-codes ids (`==1` split
   `ExchangeId::new(0)`/`StageId::new(0)/(1)`; join `0/1/2/3`; sort-over-aggregate `0/1/2`; `fragment.rs:146-148/
   247-250/794-798`). `StageId`/`ExchangeId` are `u32` wrappers — a monotonic `FragmentIdAllocator` threaded
   through recursion gives N barriers → N+ uniquely-id'd stages with correct `upstream_stages`.
2. **Terminal output must be *requested*, not mutated after (Codex — the hinge).** `TaskOutputBinding::ShuffleProducer`
   is **not just metadata**: the worker publishes the `Vec<RecordBatch>` it produces *as partitions*, so a subplan
   feeding a parent barrier must be fragmented with a requested **`ShuffleProducer` terminal output** and its
   terminal stage **wrapped in a matching `ExecutionPlan::Exchange`** (unless it's already the exchange producer for
   that output) — so it actually emits partition-shaped batches. Building a subplan as `FinalResult` then mutating
   it would be wrong.

## Decisions (promotion-locked)

**D1 — Remove the multi-global-sort cap.** Retarget `global_sort_barrier_count > 1 → MultipleGlobalSortBarriers`
from a V1 reject into positive recursive fragmentation. `global_sort_barrier_count` may remain a helper/guard but
no longer rejects `> 1`.

**D2 — Recursive split over `global_sort_barrier`.** Handle the outermost barrier by recursively fragmenting
`barrier.input` (requesting a single-partition empty-key shuffle terminal), then appending a sort stage that
consumes that shuffle, runs the barrier's `Sort`, and reapplies the 115 unary tail (`Filter`/`Project`/alias).
Recurse for inner barriers. Consume the *named primitive* recursively — no new shape matcher.

**D3 — Explicit terminal-output parameter (the hinge).** Fragmentation takes a `FragmentTerminalOutput`:
`FinalResult` (whole query) or `ShuffleProducer { exchange_id, partition_count, partition_key }` (parent barrier
needs the subplan as shuffle input). A stage requested as `ShuffleProducer` wraps its terminal plan in a matching
`ExecutionPlan::Exchange` unless it's already that exchange's producer. **Only the final stage emits `FinalResult`.**

**D4 — Threaded `FragmentIdAllocator`.** Monotonic `StageId`/`ExchangeId` allocation through recursion; **no
hard-coded `StageId::new(0)`/`ExchangeId::new(0)` on production fragmentation paths** (intentionally-fixed identity
literals like singleton `TaskId::new(0)` are exempt).

**D5 — Base fragmenters become allocator-aware + terminal-output-aware.** The no-exchange, one-exchange,
grouped-aggregate, hash-join, and broadcast-join paths allocate ids from the allocator and honor the requested
terminal output (D3), returning their terminal stage so a barrier can stack above them.

**D6 — Barriers compose *above* a fragmented join base; not *inside* join inputs.** A barrier over a join fragments
the join base first, then gathers/sorts its result. A sort barrier *inside* a hash-join input stays unsupported
(rare) — a **pinned reject**, no hidden join-input flattening.

**D7 — 115/116 behavior unchanged.** One barrier over scan / over grouped aggregate, ordered `OFFSET` (per the
stage that authored it), and unordered `Take` (transparent to barrier counting; never a `Sort` special case) all
keep their behavior.

**D8 — Reuse only.** No new logical/physical IR, executor kernel, PBC opcode, Wendelon field, merge-sort,
distributed top-k pushdown, or local preselection.

## Behavior gates

- `rune_top_k_of_top_k_fragments_and_executes` — fixture data proving "oldest 10 among top 100 by revenue" differs
  from a flattened single sort.
- `three_global_sort_barriers_allocate_unique_stage_and_exchange_ids` — ≥3 barriers → N+1 stages, monotonically
  unique `StageId`/`ExchangeId`, correct `upstream_stages` chain, **only the final stage emits `FinalResult`**.
- `grouped_aggregate_then_two_ordered_barriers_fragments` — aggregate shuffle stays distinct from sort barriers and
  composes through recursion.
- `barrier_above_join_composes_but_inside_join_input_rejects_with_pin` (D6).
- `ordered_offset_applies_per_barrier`.
- Retarget the SQL/Rune multiple-global-sort negatives → positive fragmentation tests.
- ADR 116 regression: unordered `Take` stays non-barrier, doesn't route through `Sort`.

## Architecture guards

- Retarget `adr115_multiple_global_sorts_reject_with_pin` → `adr117_multiple_global_sorts_split_recursively`.
- Strengthen `adr115_global_sort_barrier_is_a_named_primitive` — the recursive splitter **consumes** the primitive,
  no new shape matcher.
- `adr117_terminal_output_is_explicit` — a terminal-output carrier exists; shuffle terminal output wraps/produces a
  matching `ExecutionPlan::Exchange`; only the final stage emits `FinalResult`.
- `adr117_fragment_ids_are_allocated_not_fixed` — production fragment builders allocate ids; structural uniqueness
  across ≥3 stages.
- `adr117_no_barrier_inside_join_inputs`; `adr117_take_is_not_a_sort_barrier`; `adr117_no_merge_sort_or_topk_pushdown`;
  `adr117_no_sql_barrier_flattening_rewrite`.
- **Recipe (per convention):** `cargo check -p toron --all-targets` + `architecture_test adr117/adr115/adr116` +
  `sort_ordering_test` + `rune_relation_handles_test` + `group_by_test` + `distributed_process_test` + full
  `-p toron`.

## Council history

3-of-3 on the topic after **Claude conceded the streaming lead** — grounded, not headcount: transport streaming
removes Toron's `collect_all()` + Wendelon's one-frame concat but does **not** bound remote scan CPU while
`handle_table_query` pre-materializes `plan.batches`; the remote-cost-bound version is an execution-pipelined
table-query ADR with all-or-nothing status-semantics work (a 2-ADR arc, not a clean 117). **Codex contributed the
terminal-output hinge (D3)** — the implementation-correctness insight that shuffle-producing terminal stages must
*produce* partition-shaped output, not just carry the binding (so recursion requests a terminal output rather than
building-as-`FinalResult`-then-mutating). **Claude grounded the fixed-id → allocator seam (D4).** The join-base
fork resolved to **compose above the join** (D6), not the scope-to-non-join fallback; barriers-inside-join-inputs
stay a pinned reject. Gemini ratified the converged shape.

## What to do next

Streaming storage boundary (the carried 2-ADR arc: **A** transport de-collect — honest naming, no remote-cost
bound; **B** execution-pipelined remote-cost cancellation — settles all-or-nothing status); joins-around-derived /
Rune joins (would exercise barrier-inside-join-input + qualifier ergonomics); logical-type lifecycle;
declared-additive types; Rune `take_available`.

## Promotion criteria

Multiple authored ordered top-k barriers fragment into a correct multi-stage DAG by recursively consuming the 115
`global_sort_barrier` primitive; terminal output is explicit and shuffle terminal stages produce matching
partition-shaped output (only the final stage emits `FinalResult`); stage/exchange ids are allocated monotonically
(no fixed literals); each authored Rune barrier is preserved ("top-k of a top-k" correct); barriers compose above
scan/aggregate/join bases but not inside join inputs; 115/116 single-barrier, ordered `OFFSET`, and unordered
`Take` behavior unchanged; no IR/executor/Wendelon/merge-sort/top-k-pushdown change; the retargeted 115 guard +
`adr117_*` guards hold; targeted tests, full Toron suite, and `cargo check -p toron --all-targets` green. Migration:
`plans/impl/117_multiple_ordered_topk_barriers.md`.
