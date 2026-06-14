# Impl Plan — Toron ADR 232: DISTINCT over Joins

Pairs with `plans/adr/232_distinct_over_joins.md`. Lift the two conservative DISTINCT-over-joins
fences (SQL classifier + binder) so the existing 231 marker dedups join output. The marker's
unconditional all-columns `Exchange` is the dedup boundary; when that boundary sits above a join
the fragmenter must split it into an existing shuffle producer/consumer stage. No new executor
kernel, IR, or Rune change. Outer joins are in scope as the altitude proof.

Touch surface:
- `sql_planner.rs` — remove the `"DISTINCT over joins is not supported"` classifier reject
  (sql_planner.rs:3449); keep derived/EXISTS/aggregate/GROUP BY/HAVING/window/set-op/DISTINCT-ON.
- `binder.rs` — `distinct_input_unsupported_message` (binder.rs:2562): replace the
  `LogicalPlan::Join { .. }` rejection with recursive inspection of `left` and `right`; **keep**
  the `LogicalPlan::Aggregate { .. }` arm.
- `tests/group_by_test.rs` — DISTINCT-over-join behavior gates (inner, outer, `*`, dup-names,
  cross-partition, Rune parity, retained fences).
- `tests/architecture/select_distinct.rs` — retarget the 231 join-fence guard; add `adr232_*`.
- a distributed-execution fixture (existing distributed test file) — cross-partition dedup.
- `distributed/fragment.rs` — split an existing `Exchange` above a join into its own shuffle
  stage so post-join DISTINCT dedups across worker partitions.

No `ir.rs` (Distinct exists), `rune_engine.rs` (distinct() wraps any frame), `exec.rs`, or
Wendelon change.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| SQL fence to remove | `"DISTINCT over joins is not supported"` (sql_planner.rs:3449) |
| Binder fence to lift | `distinct_input_unsupported_message`, `Join { left, right, .. } => distinct_input_unsupported_message(left).or_else(|| distinct_input_unsupported_message(right))` |
| Aggregate-input fence to KEEP | same fn, `Aggregate { .. } => Some("DISTINCT over aggregate inputs…")` |
| Marker all-cols re-exchange | binder.rs:1144 `all_output_columns_program`; 1153 `key: group_expr`; `bind_bound_aggregate(.., Vec::new())` |
| Post-join exchange split | `fragment_execution_plan` detects `leading_exchange_before_join` and emits `fragment_post_join_exchange_plan` using `FragmentTerminalOutput::ShuffleProducer` |
| Existing CTAS coverage is insufficient | `query_coordinator_executes_aggregate_and_join_ctas` proves aggregate CTAS and join CTAS separately, not aggregate-over-join as one query |
| Marker transparent in join walkers | `plan_contains_join` / `logical_plan_contains_join` recurse `Distinct` |
| 231 fence guard to retarget | `adr231_distinct_first_cut_fences_joins_subqueries_and_aggregates` |

## Phase 0 — Red discriminators

`select_distinct_inner_join_dedups_projected_join_rows`;
`select_distinct_left_outer_join_dedups_after_null_padding`;
`select_distinct_join_dedups_across_partitions` (cardinal, distributed);
`rune_distinct_over_join_matches_sql_plan_and_rows`. Red on `main` (a fence fires).

## Phase 1 — Lift the two fences (the whole behavior change)

Remove the SQL classifier join reject (3449). In `distinct_input_unsupported_message`, replace
`LogicalPlan::Join { .. } => Some(...)` with recursion into both children, for example:

```rust
LogicalPlan::Join { left, right, .. } => distinct_input_unsupported_message(left)
    .or_else(|| distinct_input_unsupported_message(right)),
```

Keep `Aggregate { .. } => Some(...)` and the Scan/ScanNamed `None` base. The marker lowering
(binder.rs:1144) is unchanged. Add a fragmenter split for an existing `Exchange` encountered on
the root path above a join so the dedup exchange becomes a real shuffle boundary before final
aggregation.

## Phase 2 — Behavior coverage

Two tables with duplicate join-matched rows:
- Inner join: duplicate projected join rows collapse.
- LEFT/RIGHT/FULL outer: null-padded rows dedup by the final key (altitude proof — fails if dedup
  is pushed below the join).
- `SELECT DISTINCT *` over a join dedups the full joined output.
- Duplicate output names (`a.id`, `b.id` projected) dedup by position.
- **Distributed cross-partition:** duplicates whose join-key partitions differ still collapse
  (proves the all-cols `Exchange` re-partitions). Distributed fixture, not single-node only.
- WHERE before dedup; ORDER BY / LIMIT after dedup.
- Rune `ctx.table("a").join(b,…).project([…]).distinct()` shares the SQL twin's
  DISTINCT-over-join execution shape and row digest. Frontend-specific join replay metadata may
  differ.
- Retained fences: DISTINCT over aggregate inputs / derived / EXISTS still reject; public GROUP
  BY no-aggregate still rejects.
- Add a Rune or direct logical-plan fixture for `distinct()` over a join whose child is an
  aggregate; it must still reject with `"DISTINCT over aggregate inputs is not supported"`.

## Phase 3 — Guards

Retarget `adr231_distinct_first_cut_fences_joins_subqueries_and_aggregates` to drop the join
fence expectation (keep the rest). Add the seven `adr232_*` guards. Assert the optimizer arc
(216→230) and the 231 marker/ordinal/GROUP-BY-unrelaxed/frontend-neutral guards unchanged.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test group_by_test distinct
scripts/cargo-cgroup test  -p toron --test architecture_test adr232
scripts/cargo-cgroup test  -p toron --test architecture_test adr231
scripts/cargo-cgroup test  -p toron --test architecture_test adr230
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL — 231 retarget participates
scripts/cargo-cgroup test  -p toron --test <distributed fixture>      # cross-partition dedup
scripts/cargo-cgroup test  -p toron --lib
scripts/cargo-cgroup fmt -p toron --check
git diff --check
```

## Named falsifications (for the review)

1. Lifting only the SQL fence: `rune_distinct_over_join_matches_sql_plan_and_rows` fails (binder
   fence still fires for Rune).
2. Dedup pushed below/into the join: `select_distinct_left_outer_join_dedups_after_null_padding`
   and/or the cross-partition test fail.
3. Cross-partition duplicates survive (all-cols Exchange missing/wrong):
   `select_distinct_join_dedups_across_partitions` fails.
4. Accidentally lifting the Aggregate-input fence, including under a joined child:
   `distinct_over_aggregate_inputs_still_rejects` or
   `distinct_over_joined_aggregate_input_still_rejects` fails.
5. A new `ExecutionPlan` op or DISTINCT-specific runtime surface: the no-physical-surface guard
   fails.
6. Name-based join-output grouping: the ordinal-safe joined test fails.

## Hygiene

- No m-word; no version-suffix idiom; no new physical op / durable key / optimizer surface; no
  goldens beyond DISTINCT-over-join result rows.
- `--all-targets` confirms the 231 guard retarget compiles.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Confirm the distributed fixture actually places duplicates in different pre-dedup partitions
   (else it passes for the wrong reason); the all-cols `Exchange` is what it proves.
2. FULL outer join: two null-extended rows that match on the projected key must collapse.
3. Confirm outer-join demotion / predicate pushdown over a `Distinct(Join(...))` shape behaves
   (the join walkers already recurse `Distinct`; verify nothing mis-handles the now-reachable
   shape).
