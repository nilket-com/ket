# ADR 338 - Implementation plan: bounded streaming proof cut (Join -> Filter -> Aggregate(global), q14/q19)

Pairs with `plans/adr/338_streaming_collapsing_aggregate_segment.md`. An engine cut in `crates/toron` (exec.rs): a
special-case **chunk-fold stream** for the `Aggregate(global) <- Filter? <- InnerJoin` segment, so q14/q19 fit at
sf=1. **Bounded - no source-wide rewrite.** Verifies ADR 337's prediction (move exactly q14/q19).

## D1 - factor the global-aggregate state into init / fold / finalize
- Refactor `bound_global_aggregate` (exec.rs:1493) into reusable helpers: `init_global_agg_state(...)`,
  `fold_global_agg_state(&mut state, &batch)`, `finalize_global_agg_state(state) -> RecordBatch`.
- The **existing** materialize-each Aggregate arm now calls init -> fold over each input batch -> finalize
  (**behavior-neutral**: same rows folded in the same order, same result). Lock this with the existing aggregate
  tests staying green.

## D2 - shape detection BEFORE execute_bound(input) (or the cut is inert)
- In the Aggregate arm (exec.rs:872), **before** `execute_bound(input)`, detect the streaming segment: a **global**
  aggregate whose `input` is `InnerJoin` or `Filter(InnerJoin)` (the filter stateless), with no exchange/grouping/sort
  between.
- If matched -> take the streaming path (D3), **not** `execute_bound(input) -> Vec`. If not -> the existing path.
- The detection must read the bound `ExecutionPlan` children (no execution); the gate is the whole point - executing
  the child first materializes the output and the cut does nothing.

## D3 - the chunk-fold stream (fold-and-drop, never collect)
- Drive the join to produce its **selection** (the `Vec<JoinOutputRow>` - cheap, ~48 MB for 6M rows), then loop over
  **contiguous selection ranges** (chunk size = the existing cross-join flush threshold, exec.rs:3121-3169):
  - materialize the chunk (`materialize_join_selection` on the range),
  - apply the filter to the chunk (stateless `EvaluateInToron`),
  - `fold_global_agg_state(&mut state, &chunk)`, then **drop the chunk**.
- `finalize_global_agg_state(state)` -> the 1-row result; the outer `Project` (q14's ratio) runs on it as today.
- **Never accumulate chunks into a `Vec`** (rebuilds the wall). **Preserve selection order** across chunks
  (contiguous ranges, in order) - the correctness invariant.
- q14/q19 use the **spilled** join route at sf=1. Wire the stream at the shared selection-consumption boundary used by
  both in-memory and spilled joins: factor a helper that consumes ordered `JoinOutputRow` ranges and either
  materializes batches (existing path) or folds chunks (streaming segment). The implementation must cover
  `probe_spilled_hash_join`, not only the in-memory `execute_hash_join` branch; otherwise the real q14/q19 cells will
  not move.
- This ADR may still accumulate the selection vector first. Streaming the probe/selection itself is out of scope for
  the bounded proof cut unless Phase 3 shows the selection term, not output materialization, has become the next wall.

## D4 - correctness invariant (row order, not associativity)
- The chunks are contiguous ranges of the same selection in the same order, so the fold sees values in the **identical
  order** to full materialization -> bit-identical for **both** Decimal (strict) and **Float64 (dev, non-associative)**.
- This is what makes dev safe; a reorder would pass Decimal and silently break Float64.

## Tests (`toron` lib + `toron-tpch`)
- **Float64 + Decimal parity (the key test):** a synthetic `Aggregate(global) <- Filter <- Join` over more rows than
  the chunk threshold -> the chunked path result **equals** the full-materialization result, asserted in **both**
  Float64 (dev) and Decimal (strict) modes. (Float64 is the one that breaks if order isn't preserved.)
- **Spilled-route coverage:** force the matching synthetic through the spilled hash-join path and prove the same
  chunk-fold result. q14/q19 are spilled at sf=1, so an in-memory-only streaming test is insufficient.
- **init/fold/finalize behavior-neutral:** existing global-aggregate tests stay green (the refactor changes nothing).
- **Shape fences:** grouped aggregate / join-to-join / exchange-present / non-stateless-filter shapes take the
  **existing** path (the primitive does not engage); assert via the plan or a trace that the streaming path is not
  taken.
- **Detection-before-execute:** a test (or assertion) that the streaming segment never calls `execute_bound(input)`
  for the matched shape (else the cut is inert).
- sf=0.01 reach/correctness unchanged.

## Phase 3 - re-measure (release, capped, sf=1) - verify 337's prediction
```
scripts/cargo-cgroup build -p toron-tpch --release
# q14, q19 @ sf=1 (4G cap): expect pass@execute (was memory_budget_exhausted@execute)
# q07/q17/q21 @ sf=1: still memory_budget_exhausted (unchanged - non-goal)
```
- q14/q19 `pass@execute` (or the miss explained with the measured peak split vs 337's expected). Update the ADR 330
  atlas: q14/q19 sf=1 `memory_budget_exhausted` -> `pass@execute`.
- q07/q17/q21 unchanged (expected-partial, not opened).

## Seal gate (via `scripts/cargo-cgroup`, after `fmt`)
```
scripts/cargo-cgroup fmt
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix     # 22/22 @ sf=0.01 unchanged
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q14 --sf 1   # scale correctness (first scale-pass cut)
scripts/cargo-cgroup run -p toron-tpch -- correctness --query q19 --sf 1
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
#   release/cgroup q14/q19 sf=1 -> pass@execute (recorded); q07/q17/q21 unchanged
```

## Results doc
`crates/toron/plans/impl/338_streaming_collapsing_aggregate_segment_results.md`: the init/fold/finalize refactor; the
shape detection (before execute_bound) + the streaming chunk-fold; q14/q19 sf=1 before/after (memory_budget_exhausted
-> pass@execute) measured vs 337's expected; the Float64/Decimal parity result; q14/q19 `correctness --sf 1`
pass@compare; q07/q17/q21 unchanged; the ADR 330 atlas update; full 22/22 at sf=0.01.

## Risk / blast radius
- **Detection-before-execute** - the gate must precede `execute_bound(input)`; the detection-before-execute test
  guards it.
- **Fold-and-drop** - no chunk-collecting; the peak split (Phase 3) proves the output didn't materialize.
- **Spilled route** - the direct q14/q19 sf=1 cells spill before probing; tests must force and cover that route.
- **Order, not associativity** - the Float64/Decimal parity test is the correctness gate.
- **Shared aggregate path** - the init/fold/finalize refactor is behavior-neutral; full `--no-fail-fast` +
  `architecture_test`; existing aggregates byte-identical.
- **Scope** - the direct collapsing segment only; q07/q17/q21/q12/q18/q04/q20 untouched.

## Seal
`impl: adr 338 streaming collapsing aggregate segment`. Do not commit wip drafts.
