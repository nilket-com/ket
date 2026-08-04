# ADR 334 Results - memory-accounting escape path

## Verdict

ADR 334 names the paths and refutes the single-mechanism hypothesis. The six ADR 333 OOM cells are not all one
"hash build estimate" bug.

- **Accounted contrast (q04/q12/q20):** the clean `memory_budget_exhausted` rows come from
  `SpilledJoinBuild::write_inner`'s resident-memory check at `crates/toron/src/exec.rs:5426-5431`.
- **q18:** OOMs inside `SpilledJoinBuild::write_inner` while the spill writer is still building pages/index state,
  before the final resident-memory check can run.
- **q07/q14/q17/q19/q21:** OOM in `materialize_join_selection` (`exec.rs:3216-3245` for inner/full-width joins),
  after probe selection has completed. This path has no task-budget memory check around index arrays or Arrow
  `take` output columns.

So the next arc should split the fix scope: make join-output materialization observable/budgeted, and separately
move/account the spill-writer resident check early enough to catch q18.

## Gen-Fits Proof

ADR 333's classification still holds:

- q18 uses `q03_tables()` exactly; q03 fits under 4G, so q18's kill is execute allocation.
- q14/q17/q19 use the lineitem+part family; post-331 q08/q09 ran keyed supersets, so generation fits.
- q21 is contained by q05's table family; q05 fits under 4G.

The capped probes below are therefore execution-memory probes, not data-generation failures.

## Probe Method

Temporary env-gated trace hooks were added to `crates/toron/src/exec.rs`, run in a release binary under the same
4G cgroup cap as ADR 333, then removed before seal.

Build:

```sh
scripts/cargo-cgroup build -p toron-tpch --release
```

Run shape, repeated for q04/q12/q20/q18/q07/q14/q17/q19/q21:

```sh
systemd-run --user --same-dir --wait --collect --pipe --property=MemoryMax=4G \
  --property=MemoryAccounting=yes --property=RuntimeMaxSec=300s \
  /bin/sh -lc 'TORON_ADR334_TRACE=1 target/release/toron-tpch run --query q18 --mode strict --sf 1' || true
```

The trace hooks printed summary lines only; no full debug dumps.

## Accounted Side: q04/q12/q20

All three clean-fail through the same typed producer:
`SpilledJoinBuild::write_inner` computes `resident_without_cache`, compares it to `task_budget.memory`, and returns
`budget_exhausted(BudgetDimension::Memory, ...)` at `exec.rs:5426-5431`.

Evidence:

- q04: precheck spilled a 1,500,000-row build (`build_estimate=180000000`, `memory_cap=67108864`); spill writer
  completed 481 pages and then reported `resident_without_cache=121631060 > 67108864`.
- q12: same 1,500,000-row build and same resident estimate as q04,
  `resident_without_cache=121631060 > 67108864`.
- q20: precheck spilled an 800,000-row build (`build_estimate=102400000`); spill writer completed 367 pages and
  reported `resident_without_cache=72131060 > 67108864`.

This is the honest-red path ADR 333 surfaced as `memory_budget_exhausted`.

## Escaping Side A: q18

q18 reaches the same spill writer but dies before the writer can run the resident-memory check.

Trace sequence:

- First join builds in memory: `left_rows=150000`, `right_rows=1500000`, `build_estimate=18000000 <= 67108864`.
- Second join spills by precheck: `left_rows=1500000`, `right_rows=6001215`,
  `left_bytes=452262956`, `right_bytes=805868269`, `build_estimate=180000000 > 67108864`.
- The pre-spill build map completes: `inserted_rows=1500000`, `keys=1500000`.
- `SpilledJoinBuild::write_inner` starts with `build_keys=1500000`, `build_row_refs=1500000`.
- Last observed progress before cgroup kill:
  `pages=144`, `spill_bytes=9437760`, `writer_peak=65524`, `index_keys=449280`.
- The run ends with `oom-kill`, not a diagnostic row.

Named path: **spill-writer page/index construction is checked too late**, and q18 holds large concatenated
left/right batches plus the pre-spill build map while that writer state grows. The existing resident check catches
q04/q12/q20 only after all pages/index entries have been built; q18 reaches the cgroup ceiling before that point.

## Escaping Side B: q07/q14/q17/q19/q21

These are not q18's spill-writer-before-check path. They die after probe selection completes, while
`materialize_join_selection` builds index arrays and Arrow `take` output columns for about 6M rows.

Shared materialization evidence:

- The selection length is `6001215`, with trace-estimated selection bytes `72014580`.
- `materialize_join_selection` starts with `output_fields=21`.
- No `TaskBudget` memory check exists around the left/right index arrays, the per-column `take` calls, or the
  final output `RecordBatch::try_new`.

Per-query evidence:

- q07: in-memory build/probe completes (`left_rows=10000`, `right_rows=6001215`,
  `selection_len=6001215`). OOM occurs during `materialize_join_selection` after all small left columns are taken
  and while taking the first large right column (`right column=0`, source bytes `96019536`).
- q21: same in-memory shape as q07. OOM occurs during materialization after the small left columns and while taking
  large right-side columns (`right column=1` was the last printed trace).
- q14/q17/q19: the large lineitem+part join spills, but the spill writer passes its resident check first:
  `resident_without_cache=16331072 <= 67108864`, `pages=419`, `spill_bytes=27413240`. Spilled probe then completes
  with `selection_len=6001215`, and OOM occurs during `materialize_join_selection` while taking output columns.
  q14/q17 reached right column 5 before kill in the second trace; q19 reached right column 3.

Named path: **join output materialization is unbudgeted**. Hash build/probe may be budget-aware enough to reach
probe completion, but the full-width joined output is materialized as one batch with no memory guard or chunking.

## Six-Site Separation

- **(1) Precheck estimate:** not the escaping site for the broadened family. q18/q14/q17/q19 correctly route to
  spilled join when `estimated_build_hash_bytes > memory_cap`; q07/q21 correctly build a tiny side in memory.
- **(2) `HashMap::with_capacity(rows)`:** not the observed kill site; q07/q21 complete the in-memory build.
- **(3) Per-row growth / `BuildRowList`:** not the observed kill site; in-memory probe completes for q07/q21.
- **(4) Retained / concatenated Arrow batches:** contributes to both named paths. q18 holds large joined batches
  while spill-writer state grows; q07/q14/q17/q19/q21 hold large left/right batches while materializing output.
- **(5) Spilled fallback build map:** q18/q14/q17/q19 all build the fallback map successfully before the observed
  kill point.
- **(6) Spilled writer resident state:** q18 dies during writer page/index construction before the check; q04/q12/q20
  reach the same check and fail cleanly; q14/q17/q19 pass that check and die later in output materialization.
- **Refutation branch:** five of six escaping queries are a different operator path:
  `materialize_join_selection`.

## Follow-On Scope

Recommended next ADRs:

1. **Budget/chunk join output materialization.** Add a memory estimate or streaming/chunked output path around
   `materialize_join_selection` so q07/q14/q17/q19/q21 return a named `memory_budget_exhausted` row instead of a
   cgroup kill.
2. **Make spill-writer resident state observable before construction completes.** Move or repeat the resident-memory
   check inside `SpilledJoinBuild::write_inner` while pages/index state are growing, and account for retained
   left/right batches or pre-spill map pressure as needed, so q18 clean-fails before cgroup kill.

No engine fix landed in ADR 334.
