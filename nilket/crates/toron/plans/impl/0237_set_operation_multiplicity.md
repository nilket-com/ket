# Impl Plan — Toron ADR 237: Set Operation Multiplicity

Pairs with `plans/adr/0237_set_operation_multiplicity.md`. One new physical primitive —
row-multiplicity `Repeat` (consumes its count column, fails closed on invalid counts) — plus a
binder-only `SetOpAll` marker that lowers to count → null-safe-join → CASE → repeat using 233 grouped
`Aggregate` + 235 null-safe `Join`. Lifts the three `ALL` membership fences. Scope: `INTERSECT ALL` /
`EXCEPT ALL` / `MINUS ALL`.

## Touch surface

- `sql_planner.rs` — in `lower_set_operation` replace the three `ALL` rejects with
  `LogicalPlan::SetOpAll { kind }`; keep `BY NAME` rejected.
- `ir.rs` — binder-only `LogicalPlan::SetOpAll { left, right, kind }` plus
  `LogicalPlan::Repeat { input, count: Expr }`; `ir/execution.rs` —
  `ExecutionPlan::Repeat { input, count_column, output_schema }`.
- `binder.rs` — bind `SetOpAll` by reconciling schemas, building per-arm `Aggregate(group = all cols,
  count(*) AS lc/rc)`, null-safe `Join` (Inner / LeftOuter), `Project` computing `repeat_count` via
  `CASE`/`IS NULL`, then physical `Repeat`; bind logical `Repeat` by resolving the count expression
  to an Int64 column and setting `output_schema` = input schema minus that ordinal. Add `SetOpAll` /
  `Repeat` arms to exhaustive walkers (binder dispatch, `distinct_input_unsupported_message`,
  audit/walk utils — the 235 `Union`/D9 lesson).
- `exec.rs` — execute `Repeat`: downcast count to Int64; **error on NULL or negative**; build a
  `UInt32` take-index repeating each row by count; `arrow::compute::take` on payload columns; estimate
  output rows/bytes vs `TaskBudget.memory` before allocating; `count==0` → emit nothing.
- `distributed/fragment.rs` — `Repeat` arm in the stage-boundary / first-scan / first-join /
  exchange-count walkers; partition-local forward (no shuffle). The count `Aggregate` rides 233; the
  join rides 235.
- `tests/set_op_test.rs` or `group_by_test.rs` + `tests/distributed_process_test.rs` +
  `tests/architecture/*`.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| The three `ALL` fences to lift | `lower_set_operation` (sql_planner.rs:3292; :3323/:3330/:3332) |
| Grouped count reuse | `LogicalPlan::Aggregate { group_expr, aggr_expr }` (233) |
| Null-safe join + schema reuse | `null_safe_keys` + `build_join_plan` + `set_op_output_schema` (235) |
| min/max lowering | `CaseWhen` (sql_planner.rs:3947) + `IS NULL` + comparison + subtraction (no coalesce) |
| New set-op ALL marker + `Repeat` IR | `LogicalPlan` enum (ir.rs), `ExecutionPlan` enum (ir/execution.rs) |
| Exhaustive walkers needing a `Repeat` arm | `distinct_input_unsupported_message` (binder.rs) + fragment boundary walkers (235 D9) |
| Take + budget | `arrow::compute::take`; `TaskBudget.memory` (join/sort budget precedent) |
| Cross-producer discipline | 236 `assert_cross_producer_cardinal_is_value_discriminating` helper |

## Phase 0 — Red discriminators

`intersect_all_is_min_not_distinct`; `except_all_subtracts_exactly`;
`intersect_except_all_are_null_equal` (cardinal); `except_all_zero_count_emits_no_row`;
`repeat_rejects_null_and_negative_counts`; `set_op_all_distributed_multiplicity_across_producers`.
Red on `main` (the fences reject today).

## Phase 1 — `Repeat` primitive

`LogicalPlan::SetOpAll { kind }`, `LogicalPlan::Repeat { count: Expr }` +
`ExecutionPlan::Repeat { count_column }` (physical repeat consumes the resolved count column; output =
input minus that ordinal). Bind: count-expression resolution, Int64-column validation, and schema
surgery. Exec: Int64 downcast, fail closed on NULL/negative, `take`-index build, budget estimate,
`count==0` → no row. Add `SetOpAll` / `Repeat` arms to every exhaustive walker (compile-driven).

## Phase 2 — The desugar (lift the three fences)

In `lower_set_operation`, for `op ∈ {Intersect, Except, Minus}` + `SetQuantifier::All`, emit
`LogicalPlan::SetOpAll`. In the binder:
1. Reconcile schemas (`set_op_output_schema`) + normalize arms.
2. `Lc`/`Rc = Aggregate(arm, group = all cols, [count(*)])` — refer to the count output by ordinal /
   internal alias that cannot collide with user names.
3. `INTERSECT ALL` → Inner null-safe join + `repeat_count = CASE WHEN lc < rc THEN lc ELSE rc END`.
   `EXCEPT/MINUS ALL` → LeftOuter null-safe join + `repeat_count = CASE WHEN rc IS NULL THEN lc WHEN
   lc > rc THEN lc - rc ELSE 0 END`.
4. `Project(k…, repeat_count)` → physical `Repeat(count_column = repeat_count ordinal)`. `Minus`
   shares the `Except` arm.

## Phase 3 — Behavior coverage

- `min` / `max(0,L−R)` multiplicity (use `(1,1,1) ∩ALL (1,1) → (1,1)` to discriminate from
  DISTINCT's one-row collapse);
  NULL-equal multiplicity (cardinal); `count==0` emits nothing; `Repeat` rejects NULL/negative;
  MINUS ALL == EXCEPT ALL digest; nesting/precedence with DISTINCT-form ops; ORDER BY/LIMIT tail.
- **Distributed:** a row whose copies span >1 producer yields the correct `min`/`max` — annotate with
  the 236 cross-producer helper (e.g. `(A on W1, A on W2) INTERSECT ALL (A on W3) → (A)`); expected
  value single-producer-unreachable, not just worker count.
- Deferred fence: `INTERSECT ALL BY NAME` rejects.

## Phase 4 — Guards

Retarget the three `ALL` rejects (now lower; `BY NAME` still rejects). Add the `adr237_*` guards
(esp. `..._repeat_is_the_only_new_physical_multiplicity_primitive`, `..._repeat_rejects_invalid_counts`,
`..._desugars_to_count_join_repeat`). Assert 233/235/236 guards reused.

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test set_op_test   # (or group_by_test if folded)
scripts/cargo-cgroup test  -p toron --test distributed_process_test set
scripts/cargo-cgroup test  -p toron --test architecture_test adr237
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. Multiplicity wrong — `intersect_all_is_min_not_distinct` / `except_all_subtracts_exactly` fail
   (DISTINCT-collapse gives `(1)` not `(1,1)`).
2. NULL mishandled — `intersect_except_all_are_null_equal` fails.
3. Invalid count silently zeroed — `repeat_rejects_null_and_negative_counts` fails (must error, not
   emit 0).
4. Off-by-one at zero — `except_all_zero_count_emits_no_row` emits a stray row.
5. A set-op-ALL-specific physical op instead of `Repeat` — `..._repeat_is_the_only_new...` fails.
6. MINUS ALL diverges from EXCEPT ALL — `adr237_minus_all_aliases_except_all` fails.
7. Distributed multiplicity wrong across producers — the distributed gate fails (fixture must split a
   row's copies across producers — 236 discipline).

## Hygiene

- No m-word; no version-suffix idiom; one new physical op (`Repeat`) — guarded as the *only* new
  multiplicity primitive; goldens limited to set-op result rows.
- `--all-targets` (new IR variant → benches/examples construct `LogicalPlan`/`ExecutionPlan`); cargo
  via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Count column type: `COUNT(*)` is Int64, so Int64-only `Repeat` is sufficient; widen to all integer
   types only if binder validation stays cheap (Codex lean: start Int64).
2. Aggregate count output naming: refer by ordinal / collision-proof internal alias, not a synthesized
   user-visible name (Codex caveat).
3. Confirm `Repeat` fragments partition-local and the count `Aggregate` merges a row's count across
   producers (233 shuffle) — the distributed cardinal must split a row's copies across producers.
4. Confirm fail-closed: NULL/negative count → execution error (not 0 rows); `count==0` → 0 rows.
5. Add the `Repeat` arm to every exhaustive `LogicalPlan`/`ExecutionPlan` walker (compile-driven; the
   235 `Union` lesson) — verify with `--all-targets`.
6. Budget: estimate take-index size vs `TaskBudget.memory` before allocating (no unbounded index).
