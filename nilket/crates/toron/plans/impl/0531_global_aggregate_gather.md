# impl 531 - global-aggregate gather plan

Correctness cut: binder-side gather for global aggregates under the distributed posture, a
typed fragmenter backstop for undecomposable terminal aggregates, and the census re-check of
the q17 cell. No planner shape change, no scheduler/store change, no timing claim.

## Immutable inputs

- tree: post-guard main (`7512a9db` lineage);
- the gap: `sql_planner.rs:4014-4038` emits global aggregates with no exchange; no two-phase
  aggregation exists in the codebase;
- the construct: gather exchange (`partition_count = 1`, empty key) — emitted at
  `sql_planner.rs:3603-3612` (ORDER BY funnel) and `:8990-8998` (empty-spec window),
  fragmented generically (`fragment.rs:277-336`), proven cross-worker
  (`distributed_process_test.rs:867-897`);
- the reusable proofs: `binder/aggregate_exchange.rs:52-139`
  (`pre_aggregate_exchange_is_redundant`, `plan_partitioning_proof`), called from
  `bind_bound_aggregate` (`binder.rs:2291`);
- the backstop home: `fragment_hash_join_plan` after `fragment.rs:696`, symmetric with
  `reject_sort_barrier_inside_join_inputs` (`:657`); same gap in `fragment_broadcast_join_plan`
  (`:769`);
- the census instrument: `execution-census --query q17` at the ADR 529 envelope from a sealed
  release binary; ADR 529's classification and premise guard are unchanged.

## Step 1 - binder gather

In `binder/aggregate_exchange.rs`, add `require_global_aggregate_gather`: when the planner
config has `single_partition_execution == false` and the bound aggregate's group keys are
empty, wrap the aggregate input in a bound gather exchange (`partition_count = 1`, empty key)
unless `plan_partitioning_proof` returns `SinglePartition`. Wire it into
`bind_bound_aggregate` beside the collapse pass; thread the posture flag if the module does
not already see it. Tests: posture-off invariance (byte-identical bound plans for a scalar
aggregate over a join, and gauge-config fingerprints for q06/q17 unchanged); posture-on
insertion; no double-wrap when a gather is already present; proof-based skip for a
single-partition input.

## Step 2 - fragmenter backstop

New `FragmentError` variant (for example `TerminalAggregateNotDecomposable`) raised by a
consumer-plan walk in both join fragmenters: any aggregate above the replaced join with empty
group keys, or whose group-key roots are not covered by the join boundary's partition keys
(reuse the coverage predicate). `classify_fragment_error` in toron-tpch gains the arm by
exhaustive-match force; name the slug `fragment_terminal_aggregate_not_decomposable`. Unit
tests construct funnel-less plans for both arms; a JoinKind-style sweep is not expected, but
run the FULL `architecture_test` battery — a new error variant may still intersect
source-scanning guards, and any guard change must name ADR 531.

## Step 3 - end-to-end proof

A `distributed_process_test` case: scalar aggregate over a two-table hash join across two
worker processes, value-discriminating (the correct global sum is unreachable from any single
partition's rows). Assert the fragment shape gained the single-task terminal aggregate stage.

## Step 4 - seal and census re-check

Commit `impl: adr 531 global aggregate gather`; full battery with direct exit statuses (both
fmt checks, three `--all-targets` checks, toron lib, FULL `architecture_test`, complete
`--no-fail-fast`, full `toron-tpch`, `git diff --check`); push; release binary. Then re-run
the execution census q17 cell (and the fragment census, whose q17 shape gains a stage) at the
ADR 529 envelope. Predeclared reading:

- `executes_identical` — expected; record the new shape and digest equality here;
- a typed wall (for instance `live_shuffle_backpressure_stalled` on the new boundary) — an
  honest capability result; record it and leave the cell to the streamability seam;
- `executes_divergent` — the ADR fails; revert or fix before sealing anything.

Close with `perf: adr 531 close q17 gather` appending the cell receipt. The 529 timing block
remains in force pending q11 (ADR 532).

## Repository gates

As step 4; the battery repeats after any code change made in response.

## Implementation receipt — 2026-08-01

Sealed at `7359fd18` (first Toron engine cut in the ket tree; the frozen nilket board carried
over unchanged).

- **Step 1 (binder gather)** — `prepare_aggregate_input_exchange`
  (`binder/aggregate_exchange.rs`) composes the redundant-exchange collapse with the new
  gather pass: distributed posture + empty group keys + no `SinglePartition` proof → wrap the
  fold input in `Exchange { partition_count: 1, key: empty }`. `bind_bound_aggregate` is the
  sole `ExecutionPlan::Aggregate` constructor, so the funnel cannot be bypassed. The skip arm
  reuses `aggregate_partition_decomposable` (empty group program ⇒ decomposable iff
  single-partition proof). Local posture untouched by construction; pinned by
  `adr531_local_posture_leaves_global_aggregate_shape_untouched`.
- **Step 2 (fragmenter backstop)** — `FragmentError::JoinConsumerAggregateNotDecomposable
  { group_key_count }` raised by `reject_undecomposable_aggregate_in_join_consumer` in both
  join fragmenters; shares the binder predicate so it accepts exactly what the collapse emits
  (covered-group-keys guardrail test). Census slug
  `fragment_join_consumer_aggregate_not_decomposable` added under compile force.
- **Step 3 (end-to-end)** — `adr531_global_aggregate_over_join_gathers_across_producers`:
  two worker processes, SUM=306/COUNT=3 in one row, cross-producer value-discriminating.
  Probe-verified discriminating: with the gather edited out, the same query fails on the
  typed backstop ("0 group key roots"), not silently divergent.
- **Engaged defect fixed in-scope** — the gather over count-only inputs exposed that
  exchange repartitioning could not build zero-column routed batches;
  `repartition_exchange_batch_profiled` now uses `RecordBatchOptions::with_row_count`
  (`exec/aggregate.rs`), the existing `storage.rs` idiom.
- **Re-pinned distributed-posture shapes** (the pre-531 contracts this ADR deliberately
  changes): `scalar_aggregate_having_only_fragments_to_gathered_terminal_fold` (1 → 2
  stages), `scalar_aggregate_ordered_fragments_to_gathered_three_stage_chain` (2 → 3
  stages), and the two ordered-scalar worker-fanout pins (4-worker pool now selects 3
  unique workers — scan producer, fold, sort).
- **Battery** — both fmt checks; `--all-targets` checks for toron, toron-tpch,
  toron-client; toron lib 1473; FULL `architecture_test` 2095 (binder root kept ≤ 6,275 by
  moving the pass composition into `aggregate_exchange.rs`, no bound bump); complete
  `--no-fail-fast` exit 0 (46 suites); full toron-tpch; `git diff --check`. All green at the
  sealed identity.

## Census re-check receipt — 2026-08-01

Release binary at the clean sealed identity (`build_commit=7359fd18abd4`, no dirty flag),
strict sf=8 persisted Parquet regenerated in the ket tree (full-hash verification), join-spill
scratch cleared, ADR 529 envelope verbatim (`W=4`, `memory_per_task=536,870,912 B`,
`max_in_flight_tasks=4`, `tcp_loopback`, `in_memory` store). Artifacts:
`target/tpch/adr531_execution_census_7359fd18/execution_census.jsonl` and
`target/tpch/adr531_fragment_census_7359fd18/fragment_census.jsonl`.

| query | classification | local rows | distributed rows | digest | schema | stage widths |
|---|---|---:|---:|---|---|---|
| q17 | executes_identical | 1 | 1 | equal | equal | 1,1,4,1 |

The q17 cell left `executes_divergent` (ADR 529: 1 local row vs 4 concatenated partials) for
`executes_identical` — the predeclared expected outcome. Byte-identical Arrow IPC digests and
schema fingerprints (`fnv1a64:593a01f287d0e7d8`) on one attempt, zero transport retries. The
fragment census q17 shape gained the predicted single-task terminal fold stage
(3 stages `1,1,4` → 4 stages `1,1,4,1`, still `fragments_parallel`). Elapsed is diagnostic
only (distributed 3,754 ms vs local 4,513 ms); no timing authority is created and the 529
timing block stands pending q11 (constant-key HAVING-scalar class — its ADR number moves past
532, which the local-deployment-profile cut took during the freeze).
