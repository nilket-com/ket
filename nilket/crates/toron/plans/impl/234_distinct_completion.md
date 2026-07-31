# Impl Plan — Toron ADR 234: DISTINCT Completion

Pairs with `plans/adr/234_distinct_completion.md`. Two fence-lifts onto the existing
`LogicalPlan::Distinct`: **(A)** a pre-aggregate `f(DISTINCT x) → f(x)` desugar owned by
`lower_select`, and **(B)** DISTINCT over/inside derived tables and (inert) inside EXISTS. **No new
execution operator, IR variant or `AggregateFunction` flag, fragmenter code, Rune code, or optimizer
surface.**

## Touch surface

- `sql_planner.rs` — **A:** stop rejecting `duplicate_treatment` on the aggregate-discovery path
  (`aggregate_function_args`, ~6916); thread a planner-private `DistinctAggregateRewrite { dedup_exprs,
  rewritten_aggr_expr }` out of aggregate discovery (`lower_aggregate_projection`, 3749); in
  `lower_select` wrap the input as `Project([group ∪ {x}]) → Distinct` below the existing grouped
  (3544-3552) and global (3589) aggregate builders. Validate the one-dedup-exact scope; reject the
  fan-out cases with sharper messages. Leave `function_positional_args` (:6674) alone; it protects
  scalar helper modifiers, not aggregate DISTINCT. **B:** drop `distinct_requested &&
  table_factor_is_derived` (3449); drop `select.distinct.is_some()` from
  `validate_derived_select_shape` (7045) and `validate_exists_select_shape` (7084); ensure the EXISTS
  decorrelator builds no `Distinct`.
- `tests/group_by_test.rs` — A behavior (grouped/global/join, NULL, sum/min/max, retained fan-out
  fences) + B derived/EXISTS.
- `tests/distributed_process_test.rs` — the cardinal cross-partition `count(DISTINCT)`.
- `tests/architecture/*` — retarget the `duplicate_treatment` reject guard; add `adr234_*`.

No `ir.rs` (`AggregateFunction` unchanged — flagless), `exec.rs`, `distributed/fragment.rs`
(232 Distinct + 233 aggregate split reused as-is), `rune_engine.rs`, or Wendelon change.

## Anchors (re-ground at implementation)

| Concern | Anchor |
|---|---|
| `count(DISTINCT x)` fence (discovery path) | `aggregate_function_args` `duplicate_treatment.is_some()` (sql_planner.rs:6916; called at :4202) |
| Scalar helper modifier fence (retained) | `function_positional_args` (:6674); no ADR 234 behavior change |
| Aggregate discovery / `aggr_expr` build | `lower_aggregate_projection` (:3749), `discover_select_aggregates` (:3915) |
| Grouped aggregate assembly (insert Distinct below Exchange) | sql_planner.rs:3544-3552 |
| Global aggregate assembly (insert Distinct below Aggregate) | sql_planner.rs:3589 |
| `COUNT` ignores NULL vs `CountAll` counts all | exec.rs:387-392 vs :374 |
| Distinct node builder | `wrap_distinct_if_needed` (:42), `LogicalPlan::Distinct` (ir.rs:962) |
| Ordinal-safe dedup group keys | `all_output_columns_program(&input.schema())` (binder.rs:1144) |
| Derived / EXISTS DISTINCT fences | `validate_derived_select_shape` (:7045), `validate_exists_select_shape` (:7084) |

## Phase 0 — Red discriminators

`count_distinct_ignores_null` (cardinal); `count_distinct_grouped_dedups_per_group`;
`count_distinct_over_join_merges_across_partitions` (distributed); `global_count_distinct_no_group_by_works`;
`outer_distinct_over_derived_is_ordinal_safe`; `distinct_inside_exists_is_accepted_and_stripped`.
Red on `main`.

## Phase 1 — A: the pre-aggregate desugar (the behavior change)

1. In aggregate discovery, capture `duplicate_treatment == Distinct` + the lowered single arg expr
   instead of rejecting. Build `DistinctAggregateRewrite { dedup_exprs = group_keys ∪ {x},
   rewritten_aggr_expr }` where each `f(DISTINCT x) → f(x)` and **`COUNT(DISTINCT x) → Count(x)`
   (never `CountAll`)**.
2. Validate one-dedup-exact (D3): every SELECT+HAVING aggregate is DISTINCT, same single plain-column
   arg, fn ∈ {COUNT,SUM,MIN,MAX}. Else `unsupported` with a message naming the fan-out seam
   (mixed / multiple / AVG / computed-arg).
3. In `lower_select`, when the rewrite is present, replace `input: plan` with
   `Distinct { input: Project(plan, dedup_exprs) }` under the existing `Exchange(key=g)` (grouped) or
   directly under the global `Aggregate`; emit `rewritten_aggr_expr`. The Distinct's own dedup shuffle
   (232) co-locates `(g, x)` before the 233 group shuffle.

## Phase 2 — B: derived / EXISTS

- Remove fence 3449 (outer DISTINCT over derived) — `wrap_distinct_if_needed` already wraps; confirm
  ordinal-safe via binder.rs:1144. Remove `select.distinct.is_some()` from
  `validate_derived_select_shape` (DISTINCT inside derived) — `lower_derived_table` already calls
  `lower_select`. Keep other derived shape fences.
- Remove `select.distinct.is_some()` from `validate_exists_select_shape`; in the decorrelator, ignore
  the inner DISTINCT (build no `Distinct`). Keep GROUP BY/HAVING/join/window fences inside EXISTS.

## Phase 3 — Behavior coverage

- A grouped: `count/sum/min/max(DISTINCT x) GROUP BY g` over duplicated x per group; **NULL x** present
  (`Count(x)` correctness). Global: `count(DISTINCT x)` no GROUP BY. Over join: rides 232+233.
- **Distributed cardinal:** `count(DISTINCT x) GROUP BY g` where one group's *distinct* x values are
  produced by >1 join/scan worker — correct count proves cross-producer dedup-then-count (apply the
  233-cardinal lesson: the fixture must genuinely split a group's distinct values across workers, not
  pass for the wrong reason). Assert >1 worker.
- Retained fan-out fences: mixed / multiple-distinct-arg / `AVG(DISTINCT)` reject by message.
- B: outer DISTINCT over derived (+ the `x, x` ordinal pin); DISTINCT inside derived (marker built);
  DISTINCT inside EXISTS (digest == plain EXISTS, no Distinct in semi-join).
- Retained: `SELECT DISTINCT … GROUP BY`, `SELECT DISTINCT … <agg>`, `DISTINCT ON` still reject.

## Phase 4 — Guards

Retarget the aggregate `duplicate_treatment` reject guard (one-dedup accepted, fan-out rejected);
preserve the scalar helper modifier guard. Add the seven `adr234_*` guards (esp.
`adr234_distinct_aggregate_keeps_aggregate_function_enum_flagless` and
`adr234_count_distinct_rewrites_to_count_arg_not_count_all`). Assert 231/232 marker + ordinal-safe
guards reused, 233 aggregate/exchange guards reused, optimizer arc (216→230) unchanged. Invert ADR
115's `adr115_derived_query_tail_fence_lifted_only` (it pinned the now-lifted derived
`select.distinct.is_some()` reject).

## Verification gate

```text
scripts/cargo-cgroup check -p toron --all-targets
scripts/cargo-cgroup test  -p toron --test group_by_test
scripts/cargo-cgroup test  -p toron --test distributed_process_test distinct
scripts/cargo-cgroup test  -p toron --test architecture_test adr234
scripts/cargo-cgroup test  -p toron --test architecture_test adr231
scripts/cargo-cgroup test  -p toron --test architecture_test adr232
scripts/cargo-cgroup test  -p toron --test architecture_test adr233
scripts/cargo-cgroup test  -p toron --test architecture_test          # FULL — retarget participates
scripts/cargo-cgroup test  -p toron --lib
cargo fmt -p toron -- --check
git diff --check
```

## Named falsifications (for the review)

1. NULL miscount — `count_distinct_ignores_null` fails if the rewrite is `CountAll`/`count(*)` (the
   trap).
2. Dedup skipped — `count_distinct_grouped_dedups_per_group` overcounts.
3. Cross-producer dedup broken — distributed cardinal wrong total (and the fixture must split a
   group's distinct values across workers, else it passes for the wrong reason — the 233-cardinal
   discipline).
4. A new `AggregateFunction` flag/variant — `adr234_..._enum_flagless` fails.
5. Fan-out accepted — mixed / multiple-distinct / AVG(DISTINCT) acceptance fails the retained-fence
   guards.
6. Scalar helper modifiers accidentally accepted — `function_positional_args` still rejects
   duplicate-treatment/clauses for IF/COALESCE/NULLIF-style helpers.
7. Derived DISTINCT resolved by-name not by-ordinal — `outer_distinct_over_derived_is_ordinal_safe`
   fails on the `x, x` fixture.
8. EXISTS builds a Distinct — `distinct_inside_exists_is_accepted_and_stripped` finds a Distinct in
   the semi-join path / digest differs.

## Hygiene

- No m-word; no version-suffix idiom; no new physical op / IR flag / durable key / optimizer surface;
  goldens limited to distinct-aggregate result rows.
- `--all-targets` confirms the guard retarget compiles.
- Cargo via `scripts/cargo-cgroup`.

## Open for the implementing session (verify-first)

1. Confirm the `Distinct` sits *below* the group-key `Exchange` (dedup `(g,x)` before group shuffle),
   and that `lower_aggregate_projection`'s catalog / final-projection remap references the rewritten
   `Count(x)` output cleanly — the one seam Codex and Claude flagged.
2. Confirm `MIN/MAX(DISTINCT x)` produce identical results to plain `MIN/MAX(x)` (distinct-inert) so
   they are pure surface completion.
3. Confirm the distributed fixture genuinely splits one group's *distinct* x values across >1 worker
   before the dedup shuffle (else the cardinal passes for the wrong reason — the ADR 233 lesson).
4. Confirm removing `select.distinct.is_some()` from the EXISTS validator leaves the decorrelation
   path building no `Distinct` (accept-and-strip, not accept-and-execute).
