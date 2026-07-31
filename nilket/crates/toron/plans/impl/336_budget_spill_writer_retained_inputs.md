# ADR 336 - Implementation plan: budget the spill-writer retained footprint (q18)

Pairs with `plans/adr/336_budget_spill_writer_retained_inputs.md`. An engine cut in `crates/toron` (exec.rs spill
path): a **spill-entry resident-footprint guard** so q18 clean-fails `memory_budget_exhausted@execute` instead of
`oom-kill`. **Observability, not fit.** The cap is **measured from the green-spiller gap**, not assumed.

## Phase 0 - measure first (the cap is data-driven)
- Instrument the spill path (temporary, env-gated, removed before seal - the ADR 334 pattern) to print, at spill
  entry: `left_bytes` + `right_bytes` (`get_array_memory_size`), the fallback build-map resident estimate, and which
  query reached the spill path + completed.
- Run **all green controls** (q01/q03/q05/q06/q10/q13/q16/q22) + q18 @ sf=1 (release, 4G cap). Record: which greens
  **spill-complete** and their spill-entry footprint; q18's footprint (expect ~1.26 GB inputs + the 1.5M-key map).
- **Set the cap from the measured gap:** largest green spill-completer footprint < cap < q18's footprint. If no green
  spills-complete, use a generous fixed cap well below q18 (document the headroom). **Do not** default to raw
  `task_budget.memory` or 335's 16x without the measurement justifying it.

## D1 - the guard (exec.rs `execute_spilled_hash_join`)
- Placement: **after the fallback build map exists (exec.rs:3915-3932) and before `SpilledJoinBuild::write(...)`** -
  the kill point precedes page/index construction.
- Footprint = `left_batch.get_array_memory_size()` + `right_batch.get_array_memory_size()` (exact; count both
  retained inputs for every spilled hash join kind) + the fallback build-map resident estimate (keys, vec
  headers/capacity, row-index capacity, and hash-table overhead). Saturating arithmetic.
- If `footprint > spill_retained_cap`, `return Err(budget_exhausted(BudgetDimension::Memory, footprint,
  spill_retained_cap))` before `write`.
- Leave the existing `write_inner` resident check (exec.rs:5535-5542) intact - it stays the q04/q12/q20 producer;
  336 adds the **earlier** spill-entry guard for the retained-input term it omits.

## D2 - the cap constant (from Phase 0)
- A named constant (or task-budget-derived value) set from the measured gap, with a comment citing the green-spiller
  footprint vs q18's footprint (the 335 pattern: document why the threshold is where it is). Reuse 335's
  `join_output_materialization_memory_cap` **only if** the measurement shows it cleanly covers the retained-input
  term; otherwise a distinct `spill_retained_*` cap.

## Tests (`toron` lib)
- **Guard fires (clean fail):** a synthetic spilled join whose retained inputs + build map exceed a small cap ->
  `execute_spilled_hash_join` returns `budget_exhausted(Memory, ...)` **before** `write`; assert the reason.
- **Green spilled join unchanged:** a spilled join whose footprint fits -> completes with the same rows.
- **Counts both terms:** a case where inputs alone fit but inputs + build map exceed the cap -> fires (proves the
  build-map term is counted, not just inputs).
- **Retained-input side rule:** both concat'd input batches are counted for every spilled hash join kind. Do not copy
  ADR 335's preserved-output-side rule for semi/anti joins; those inputs are still resident on the spill path.
- sf=0.01 reach/correctness stay green (the guard never fires at sf=0.01).

## Phase 3 - re-measure (release, capped, sf=1)
```
scripts/cargo-cgroup build -p toron-tpch --release
# systemd-run ... -p MemoryMax=4G -p RuntimeMaxSec=600s target/release/toron-tpch run --query q18 --mode strict --sf 1
#   -> fail(memory_budget_exhausted@execute), not oom-kill
# green controls q01/q03/q05/q06/q10/q13/q16/q22 -> all pass@execute (no false-trip)
```
- q18 emits a clean `fail(memory_budget_exhausted@execute)` row (ADR 332 retention).
- Update the ADR 330 atlas: q18 sf=1 `OOM-guard-gap` -> `memory_budget_exhausted`. **The memory family is now fully
  observable** (no silent OOM at sf=1).

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
git diff --check        # the Phase-0 instrumentation is removed
#   release/cgroup q18 -> memory_budget_exhausted; green controls -> pass (recorded)
```

## Results doc
`crates/toron/plans/impl/336_budget_spill_writer_retained_inputs_results.md`: the Phase-0 spill-path measurement
(which greens spill-complete + footprints, q18's footprint); the cap set from the gap (with the numbers);
q18 sf=1 before/after (oom-kill -> memory_budget_exhausted); the green controls staying green; the ADR 330 atlas
update (memory family fully observable); the streaming-executor wall named; sf=0.01 boards unchanged.

## Risk / blast radius
- **Cap from measurement** - the discriminating test + the Phase-0 numbers are the gate; an assumed cap is the main
  risk.
- **Both terms counted** - inputs and the fallback build map; the "counts both terms" test proves it.
- **Shared spill path** - full `--no-fail-fast` + `architecture_test`; green spilled joins byte-identical.
- **Scope** - observability only; not 335's output path, not the streaming executor (the named next arc).

## Seal
`impl: adr 336 budget spill writer retained inputs`. Do not commit wip drafts.
