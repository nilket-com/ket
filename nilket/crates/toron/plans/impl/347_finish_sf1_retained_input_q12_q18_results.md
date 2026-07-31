# ADR 347 results - retained-input streamed spill, q12/q18

## Verdict
ADR 347 cleared the retained-input memory wall for both targeted shapes, but only q12 greened.

- q12: `fail(memory_budget_exhausted@execute)` at the retained-spill guard -> `pass@execute` and `pass@compare` at `sf=1`.
- q18: retained-input memory wall cleared, then exposed a new named spill-volume wall:
  `fail(spill_budget_exhausted@execute)`, producer `streamed inner join build spill writer`,
  requested `537,631,256`, available `536,870,912`.

No memory or spill cap was raised.

## Phase 0 seam verdict
The "bare 345 reuse" premise was false for regular inner joins: the old `SpilledJoinBuild` stored key -> row indexes only, while regular inner
materialization needs build columns on a probe match. The implemented seam is the dependent branch:

- `SpilledInnerJoinBuild` writes build input batches to compressed IPC spill pages one batch/chunk at a time.
- The index stores `JoinKey -> (page_index, row)` refs.
- Materialization reads only the needed pages and uses Arrow `interleave` for spilled build rows plus `take` for probe rows.
- q12's filtered visitor now engages before `concat_or_empty`, filters each materialized chunk, and folds the grouped aggregate.

q18 refined the scope: its grouped-membership wall is `LeftSemi`, not regular inner. The cut adds a residual-free `LeftSemi` streamed-spill path that
spills the preserved/build side, probes the existence side, deduplicates matched preserved refs, and materializes preserved rows from spill pages in
build order.

## Measured rows
Baseline retained-input walls:

- q12 before: `memory_budget_exhausted`, producer `join spill retained entry`, requested `1,136,180,270`, available `1,073,741,824`.
- q18 before this cut reached the retained-entry producer at `2,791,071,873` requested / `1,073,741,824` available.

After:

```text
q12 sf=1 strict run        pass@execute  elapsed_ms=3211.9
q12 sf=1 correctness      pass@compare  elapsed_ms=6581.1
q18 sf=1 strict run        fail(spill_budget_exhausted@execute)
                            producer="streamed inner join build spill writer"
                            requested=537,631,256 available=536,870,912
```

q18's remaining wall is no longer a retained-memory/OOM-observability problem. It is the next scale wall for spill volume.

## Tests run
```text
scripts/cargo-cgroup check -p toron --lib
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup check -p toron-tpch --all-targets
scripts/cargo-cgroup fmt -- --check
scripts/cargo-cgroup test -p toron --lib streamed_spilled_inner_join -- --nocapture
scripts/cargo-cgroup test -p toron --lib streamed_spilled_left_semi -- --nocapture
scripts/cargo-cgroup test -p toron --lib execute_bound_inner_join_spill -- --nocapture
scripts/cargo-cgroup test -p toron --lib streaming_join_filter_grouped_aggregate -- --nocapture
scripts/cargo-cgroup test -p toron --lib
scripts/cargo-cgroup test -p toron --test architecture_test
scripts/cargo-cgroup test -p toron --no-fail-fast
scripts/cargo-cgroup test -p toron-tpch spill_budget_exhausted -- --nocapture
scripts/cargo-cgroup test -p toron-tpch
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q12 --mode strict --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q12 --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- run --query q18 --mode strict --sf 1
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q12 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch --release -- correctness --query q18 --sf 0.01
scripts/cargo-cgroup run -p toron-tpch -- correctness-matrix
scripts/cargo-cgroup run -p toron-tpch -- matrix
git diff --check
```

Default-scale correctness remains green for both q12 and q18. q12 also passes scale correctness at `sf=1`; q18 does not reach compare at `sf=1`.
The canonical correctness matrix remains 22/22. The reachability matrix shows q12 strict `pass@execute` and q18 strict
`fail(spill_budget_exhausted@execute)`.

## Scope notes
- Core engine symbols/comments remain shape-named; query-number scope stays in `toron-tpch` and plan/results docs.
- q04/q20 remain ADR 348.
- q18's next cut is spill-volume reduction or spill-budget policy, not retained-input memory accounting.
