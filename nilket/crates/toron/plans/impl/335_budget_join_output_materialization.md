# ADR 335 - Implementation plan: budget the join output materialization (observability-first)

Pairs with `plans/adr/335_budget_join_output_materialization.md`. An engine cut in `crates/toron` (exec.rs):
a **pre-take output-materialization budget guard** so q07/q14/q17/q19/q21 clean-fail
`memory_budget_exhausted@execute` instead of `oom-kill`. **Observability, not fit** - the executor materializes each
operator's full output (incl. Exchange), so chunking can't fit; the streaming executor is the named next arc.

## D1 - the pre-take guard (exec.rs `materialize_join_selection`, ~3199)
- The selection vector (`selection: &[JoinOutputRow]`) already exists when `materialize_join_selection` runs. **Before
  the per-column `take`**, estimate the output footprint:
  - Build the estimate from the actual source batches and selected row count, not from a worst-case schema constant.
    For each materialized side, use the source columns' bytes-per-row average
    (`column.get_array_memory_size() / source_rows`, rounded up/saturating) multiplied by `selection.len()`.
  - Add the `take` index arrays: both left/right indices for inner/cross/full-width outer joins, only the preserved
    side for semi/anti joins.
  - Count only the sides the join kind emits: both sides for `Inner`/`Cross`/`LeftOuter`/`RightOuter`/`FullOuter`,
    left for `LeftSemi`/`LeftAnti`, right for `RightSemi`/`RightAnti`.
- Wire the budget in: pass `task_budget` (or `memory_cap: u64`) into `materialize_join_selection` (its callers
  exec.rs:3092/3842/3936 already hold the budget), or guard at those call sites. Prefer the param so the check lives
  at the proven kill point. If the shared signature changes, also thread the cap through `execute_cross_join_match_all`
  for its internal CROSS chunks (exec.rs:3147/3159); that helper is called from `execute_hash_join`, where the budget
  is available, but it does not currently accept it.
- If `output_bytes > task_budget.memory.get()`, `return Err(budget_exhausted(BudgetDimension::Memory, output_bytes,
  task_budget.memory.get()))` before any `take`.
- Keep it a **pre-take guard** - do not add whole-join accounting or touch the spill-writer path (ADR 336).

## D2 - estimate precision (the discriminating edge)
- Tight enough: q07/q14/q17/q19/q21's ~6M-row x 21-field output (ADR 334: `selection bytes ~= 72 MB` > 64 MB budget)
  must trip the guard before the take OOMs.
- Loose enough: the green-at-sf=1 controls (q01/q03/q05/q06/q10/q13/q16/q22) must **not** trip it. Do not assume
  max-width for every Utf8 column; use actual source-column byte sizes / row averages so the estimate is tied to the
  batch being materialized.
- If exact selected-row bytes are too expensive to compute before the take, use the source column average with
  saturating arithmetic and record the precision tradeoff in the results doc.

## Tests (`toron` lib + `toron-tpch`)
- **Guard fires (clean fail):** a synthetic join whose materialized output exceeds a small `task_budget.memory` ->
  `materialize_join_selection` returns `budget_exhausted(Memory, ...)`; assert the reason, not an OOM.
- **Green join unchanged (no false-trip):** a join whose output fits -> materializes normally, same rows as today.
- **Discriminating:** the over-budget case must fail at the pre-take guard, before index-array or per-column `take`
  allocation; the green case must pass with the guard in place.
- **Join-kind coverage:** semi/anti joins estimate only the preserved side; full-width joins estimate both sides; CROSS
  chunks still use the shared guard after the budget/cap is threaded through `execute_cross_join_match_all`.
- sf=0.01 reach/correctness stay green (the guard never fires at sf=0.01).

## Phase 3 - re-measure (release, capped, sf=1)
```
scripts/cargo-cgroup build -p toron-tpch --release
# for q in q07 q14 q17 q19 q21: systemd-run ... -p MemoryMax=4G -p RuntimeMaxSec=300s \
#   target/release/toron-tpch run --query $q --mode strict --sf 1   (expect fail(memory_budget_exhausted@execute), not oom-kill)
```
- Each now emits a clean `fail(memory_budget_exhausted@execute)` row (a diagnostic row exists - ADR 332 retention).
- Update the ADR 330 atlas: q07/q14/q17/q19/q21 sf=1 `OOM-guard-gap` -> `memory_budget_exhausted` (observable).
- q18 unchanged (ADR 336).

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
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix   # 22/22 @ sf=0.01 unchanged
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
#   release/cgroup q07/q14/q17/q19/q21 sf=1 -> memory_budget_exhausted (recorded)
```

## Results doc
`crates/toron/plans/impl/335_budget_join_output_materialization_results.md`: the pre-take guard; the estimate formula
+ its precision evidence (fires for the five, spares the green joins); q07/q14/q17/q19/q21 sf=1 before/after
(oom-kill -> memory_budget_exhausted); the ADR 330 atlas update; the streaming-executor wall named as the next arc;
sf=0.01 boards unchanged; q18 deferred to ADR 336.

## Risk / blast radius
- **Observability, not fit** - the cut turns silent OOMs into named fails; it does not make the cells pass (that's
  the streaming arc).
- **Estimate precision** - the discriminating test (fires for over-budget, spares green) is the gate; a sloppy
  estimate is the main risk.
- **Shared path** - `materialize_join_selection` is on the common join output path; full `--no-fail-fast` +
  `architecture_test`; green joins must be byte-identical in output.
- **Scope** - pre-take guard only; not chunking, not q18, not the streaming executor.

## Seal
`impl: adr 335 budget join output materialization`. Do not commit wip drafts.
