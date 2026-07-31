# ADR 335 - budget the join output materialization (observability-first)

## Status
Proposed.

## Context

The memory arc's first **fix** cut (after the 333/334 grounding). ADR 334 named the highest-leverage escape:
q07/q14/q17/q19/q21 (5 of 6 cells) OOM in unbudgeted `materialize_join_selection` (exec.rs:3199) while building a
~6M-row output - **after** probe selection completes, during the per-column `take`. q04/q12/q20 already clean-fail
(the spill-writer resident check); q18 is the *separate* spill-writer/input-retention path (ADR 336).

### Phase 0 answered the chunk-vs-fit fork: NO (so the scope is observability, not fit)
**Can join output leave `materialize_join_selection` in bounded batches without a downstream node re-gathering it? No.**
- The executor is **materialize-each-operator**: every `execute_*` returns `Result<Vec<RecordBatch>>`
  (exec.rs:250/277/2946/3130/...); `execute_bound` and the toron-tpch reach path materialize **end to end,
  including Exchange** (confirmed locally). A parent runs a child to its full `Vec<RecordBatch>`, then operates.
- The join holds its **full materialized inputs** (ADR 334 q18: `left 452 MB` + `right 805 MB` held) and returns its
  full output as `Vec<RecordBatch>`.
- So chunking `materialize_join_selection` would **not** lower the peak - the caller re-gathers the chunks and the
  downstream gets the whole output. Chunking would **overclaim green cells.**

The honest, still-high-value cut: make the wall **observable** (the 333/334 discipline), and name the real fit-wall.

## Decision - a pre-take output-materialization budget guard (not complete join accounting)

Add a `task_budget.memory` check **before** the `take` in `materialize_join_selection`, using the **already-known
selection vector**:
- Estimate the imminent materialization footprint from the actual source batches and `selection.len()`: the left/right
  index arrays that `take` builds, plus the selected output columns. Use source-column bytes scaled by selected rows
  (or an equivalent measured average for variable-width columns), not a worst-case schema constant.
- Count only the sides that the join kind materializes: both sides for inner/cross/full-width outer joins, left side
  for left semi/anti, right side for right semi/anti.
- If the estimate exceeds `task_budget.memory`, `return budget_exhausted(BudgetDimension::Memory, ...)` **before** the
  per-column `take` / `RecordBatch::try_new` allocations balloon.
- This is a **pre-take guard**, deliberately *not* whole-join memory accounting: the selection already exists, the
  five target cells survive it and die in the take, so the guard sits exactly at the proven kill point.

Result: q07/q14/q17/q19/q21 move from silent `oom-kill` to a clean `fail(memory_budget_exhausted@execute)` row -
observable in-engine, the same honest-red q04/q12/q20 produce.

## The named next wall (no overclaim)
The reason chunking can't fit these cells **is** the next arc: the **materialize-each-operator executor model** (full
inputs + outputs retained per operator, end to end including Exchange). Making these cells *pass* needs a
**streaming/pipelined executor** - a major arc, named here, not attempted. ADR 335 makes the wall observable; the
streaming arc makes it fit.

## Acceptance - exactly the observability scope
- q07/q14/q17/q19/q21 @ sf=1 return `fail(memory_budget_exhausted@execute)` instead of `oom-kill`, under the default
  budget + 4G cgroup cap.
- **Discriminating + precise:** the guard fires for these but does **not** false-trip the green-at-sf=1 joins
  (q01/q03/q05/q06/q10/q13/q16/q22 stay `pass`). A too-loose estimate (still OOMs) or a too-tight one (fails a green
  cell) both fail acceptance.
- The **streaming-executor wall is named** as the next arc (the real fit path); ADR 335 does **not** claim to fit.
- sf=0.01 boards unchanged (reach 22 / correctness 22); q18 stays ADR 336.

## Risks
- **No overclaim** - this is observability (clean-fail), not fit; the dual must not promise green cells the
  materialize-each model can't deliver.
- **Precision of the estimate** - it must be tight enough to fire before OOM yet loose enough to spare green joins;
  the discriminating test (over-budget synthetic fails clean; green join unchanged) is mandatory.
- **Pre-take placement** - the guard sits before the `take` (the proven kill point), using the existing selection;
  it is not a broad join-operator refactor.
- **Scope fences** - not chunking (Phase 0 = no), not q18's spill-writer path (ADR 336), not the streaming executor
  (the named next arc).

## Forward arc
ADR 336 = the q18 spill-writer/input-retention path (different mechanics - `resident_without_cache` omits retained
batches). After 335/336 the six cells fail **cleanly + named**; the **streaming-executor arc** is then the
substantive scale work (turn named fails into passes), sized against observable walls. Make the wall observable
first, then make it fit.

## Why toron-home
Engine cut in `crates/toron` (exec.rs `materialize_join_selection` + the budget check); driven via `toron-tpch`
reach at sf=1. No Wendelon / cross-ring surface.

## Council provenance
Operator + Codex + Claude (convergent: observability-first). Phase 0 grounding: the executor is
materialize-each-operator end to end (Codex confirmed `execute_bound`/toron-tpch incl. Exchange), so chunking would
overclaim - the honest cut is a budget guard. Codex's sharpening folded: a **pre-take** output-materialization guard
using the existing selection vector, not complete join accounting (the five cells survive selection, die in the
take). The streaming executor is named as the real fit-wall, not attempted here.
