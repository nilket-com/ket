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
