# ADR 106: Derived Aggregate Tables

**Status:** Promoted (plan). **Genuine 3-of-3 (Claude, Codex, Gemini)** — converged on the **narrow
fence-lift** (admit grouped + global aggregate SELECTs as derived sources by relaxing the aggregate
conditions of the existing derived-select validator; keep `lower_select`, do not delegate to `lower_query`).
**Toron-only** (SQL frontend); no IR/binder/executor/PBC/Wendelon/coordinator/Rune/`information_schema`
surface and no storage *contract* change — the planner still emits `LogicalPlan::Aggregate`. One adjacent
in-memory-scan correctness fix (empty-projection row-count) the global-aggregate case surfaces travels with
it (§Adjacent fix). **Predecessors:** 100/103/104 (derived-source +
alias rename), 105 (adjacent-`Project` coalescing), the aggregate substrate (011/015/016/023). **Migration:**
`plans/impl/106_derived_aggregate_tables.md`.

## Doctrine — a planner fence-lift, not new machinery

`FROM (SELECT k, COUNT(*) AS n FROM t GROUP BY k) AS p` should bind and run on the **existing** aggregate
substrate. `lower_select` already lowers grouped *and* global aggregates to `LogicalPlan::Aggregate`
(Exchange-wrapped for grouped) → optional `Filter` for `HAVING` → optional final `Project`; the binder binds
`Aggregate`; the executor runs it; `LogicalPlan::Alias` + `bind_alias_column_rename` (104) rename *any* bound
input's columns. The only thing rejecting aggregate-derived tables is an artificial frontend fence. 106 lifts
that fence; the sole added code is one adjacent in-memory-scan correctness fix the global-aggregate case
surfaces (§Adjacent fix).

## Scale boundary

One derived-source sub-feature: aggregate-rooted (grouped + global) derived tables. **In scope:**
`FROM (SELECT k, COUNT(*) AS n FROM t GROUP BY k) AS p`, `FROM (SELECT COUNT(*) AS n FROM t) AS p`, inner
`HAVING`, alias column lists over aggregate outputs, and outer projection/filter/aggregation/top-level
ordering over the derived output. **Out of scope (stay fenced, named follow-ons):** query-level
`ORDER BY`/`LIMIT`/`OFFSET` *inside* the derived subquery (a distributed barrier + ordering interaction, ADR
003/010), joins around/inside derived, nested derived, CTE, set-ops, windows, `DISTINCT`, lateral/sample.

## Grounding (verified)

- **Two fences block aggregates, both localized.** `lower_derived_table` (`sql_planner.rs:3317`) runs
  `validate_derived_query_shape` → `validate_derived_select_shape` → `lower_select`, then rejects if
  `aggregate_output_catalog.is_some()` (`:3328`). `validate_derived_select_shape` (`:5218-5259`) is the
  **single structural gatekeeper**: it rejects `GROUP BY` (`group_by_is_empty`, `:5224`), `HAVING`
  (`:5228`), aggregate projection (`projection_contains_aggregate_call`, `:5240`) — *and* the structural
  fences `from.len() != 1` (`:5245`), joins-inside (`!source.joins.is_empty()`, `:5251`), nested-derived
  (`TableFactor::Derived`, `:5256`), non-table source (`:5257`).
- **`lower_select` already builds the whole aggregate plan.** Grouped `:2642-2693`, global `:2696-2736`
  construct `Aggregate` → `Filter` (HAVING applied **inside**, `:2673`/`:2714`) → `Project`, returning
  `aggregate_output_catalog: Some(...)`.
- **The catalog is discardable at the derived boundary.** It is consumed only by `lower_query` for ORDER BY
  resolution (`:2460`, `add_hidden_sort_projection` `:2463`). Derived ORDER BY stays fenced by
  `validate_derived_query_shape`, and HAVING is already in the plan — so discarding it loses nothing.
- **Keep `lower_select`; do not delegate to `lower_query`** (the load-bearing scope decision). The
  structural fences live in `validate_derived_select_shape`, which runs *before* `lower_select`;
  `lower_select` itself recurses through `lower_table_factor`, so the nested-derived rejection (`:5256`) is
  the only thing stopping that recursion. Delegating to `lower_query` would bypass the validator and
  re-admit nested-derived, joins-inside, and `EXISTS` decorrelation. Relaxing only the validator's aggregate
  conditions preserves every structural fence.
- **Alias rename is already generic.** `bind_alias_column_rename` builds an ordinal-identity `Project` over
  *any* bound input (104), so `AS p(pid, total)` renames aggregate outputs. Where `lower_select` emitted a
  final `Project`, the alias rename `Project` stacks on it and **ADR 105 coalesces the pair**; over a bare
  `Aggregate` the rename `Project` stays (105 does not fold `Project`-over-`Aggregate`).
- **Aggregate outputs are non-transparent for elision** by inheritance — `stored_refinement_receipt` stops
  at `Aggregate` (not a `Scan { projection: None }`), so `WHERE p.n > 1` over a derived aggregate does not
  elide. No new rule.

## Adjacent fix — empty-projection row-count (travels with 106, discipline #6)

The global-aggregate-derived case (`SELECT n FROM (SELECT COUNT(*) AS n FROM t) AS p`) is the first SQL path
that pushes an **empty column projection** to the scan: `COUNT(*)` references no column, so
`optimize_for_required` narrows the inner scan's projection to zero columns. Arrow's `batch.project(&[])`
builds a zero-column batch that **loses the row count**, so `COUNT(*)` would return `0` instead of the true
row count. The fix is a generic `project_record_batch` helper in `storage.rs` `scan_in_memory_batch`: for an
empty projection, build the zero-column batch via `RecordBatch::try_new_with_options` with
`RecordBatchOptions::with_row_count(num_rows)`; otherwise unchanged. It is **behavior-correcting and generic**
(any zero-column scan projection), surfaced *by* this feature, so it travels with the ADR (discipline #6) —
not a new storage contract/API. The `adr106_no_..._storage_..._surface` guard polices ADR106 feature-marker
leakage (not raw diffs), so it stays green: the helper carries no feature marker. Mirrored in the test
`FixtureScan` (`group_by_test.rs`), which the `global_aggregate_derived_table_binds_and_executes` gate
(`n = 6`) exercises.

## Decisions (promotion-locked, 3-of-3)

**D1 — Admit aggregate-rooted derived tables** (grouped + global).

**D2 — Relax only the aggregate conditions in `validate_derived_select_shape`.** Remove the `GROUP BY`
(`group_by_is_empty`), `HAVING`, and `projection_contains_aggregate_call` rejections. **Keep**
`projection_contains_window_function` and every structural/non-aggregate fence
(distinct/top/into/lateral/prewhere/cluster/distribute/sort_by/window/qualify/flavor, `from.len() != 1`,
joins-inside, nested-derived, non-table source).

**D3 — Drop the post-`lower_select` aggregate rejection** in `lower_derived_table` (`:3328`); discard
`aggregate_output_catalog` (safe per Grounding).

**D4 — Keep the validator skeleton; do not delegate to `lower_query`.** `lower_derived_table` still calls
`validate_derived_query_shape` → `validate_derived_select_shape` → `lower_select` → `alias_plan`.

**D5 — Reuse the existing aggregate substrate.** No new `LogicalPlan`/`ExecutionPlan` variant, binder
semantics, executor feature, aggregate kernel, or PBC/Wendelon/coordinator/`information_schema` surface, and
no storage *contract* change (the empty-projection row-count fix is an adjacent in-memory-scan correctness
fix, §Adjacent fix — not a storage-contract/feature change). The planner still emits `LogicalPlan::Aggregate`.

**D6 — HAVING stays an inner aggregate concern.** Inner `HAVING` is lowered against the inner aggregate
catalog inside `lower_select` (the existing "HAVING requires GROUP BY or aggregate projection" rule `:2582`
still applies within the subquery). Outer predicates bind only against the derived output schema.

**D7 — Alias column lists stay generic.** `AS p(bucket, total)` over an aggregate derived table flows
through `LogicalPlan::Alias` + `bind_alias_column_rename` (104); no aggregate-local rename. 105 coalesces a
redundant adjacent `Project`.

**D8 — Aggregate outputs are not refinement-transparent** (inherited; the receipt walk stops at the
aggregate boundary).

**D9 — Reconcile the inverted guards (discipline #4).** Lifting the aggregate fences changes invariants two
guards pin:
- Retarget `adr100_derived_table_source_is_narrow_not_general_subquery_support`: drop
  `"projection_contains_aggregate_call"` and `"group_by_is_empty"` from its `validate_derived_select_shape`
  required-marker set (those fences are gone); **keep** the structural markers
  (`projection_contains_window_function`, `select.from.len() != 1`, `!source.joins.is_empty()`,
  `TableFactor::Derived`, `unsupported derived table source`) and the query-shape/derived-arm markers.
- Retarget the behavior test `derived_table_source_scope_rejects_general_subquery_features`: remove the two
  aggregate-derived reject cases (global `SELECT COUNT(*) …` and grouped `… GROUP BY …`); keep
  missing-alias, lateral, nested-derived, joins-inside, joins-around, derived ORDER BY/LIMIT, `WITH`, `UNION`.

## Behavior gates

- `aggregate_derived_table_binds_and_executes` — grouped aggregate derived table feeds outer projection +
  filter and executes against the existing aggregate fixture.
- `global_aggregate_derived_table_binds_and_executes` — scalar aggregate derived table executes as a one-row
  source.
- `aggregate_derived_alias_columns_rename_outputs` — `AS p(pid, total)` renames aggregate outputs via the
  104 ordinal-identity path; no redundant adjacent `Project` survives where a final projection exists (105).
- `aggregate_derived_having_binds_and_executes` — inner HAVING resolves against the inner aggregate catalog
  and executes over the derived source.
- `aggregate_derived_outputs_are_not_refinement_transparent` — an aggregate output column gains no
  stored-refinement provenance (`find_empty_relation` None, `contains_scan` true).
- Retargeted `derived_table_source_scope_rejects_general_subquery_features` (D9).

## Architecture guards

- `adr106_derived_aggregates_reuse_existing_aggregate_ir` — no new IR/exec variant; planner emits
  `LogicalPlan::Aggregate` for derived aggregates.
- `adr106_lower_derived_table_lifts_only_aggregate_fence` — `lower_derived_table` still calls
  `validate_derived_query_shape` + `validate_derived_select_shape`; no `lower_query` delegation.
- `adr106_general_derived_source_fences_remain` — `validate_derived_select_shape` retains nested-derived,
  joins-inside, and non-table-source rejection; `validate_derived_query_shape` retains ORDER BY/LIMIT/WITH.
- `adr106_alias_column_rename_remains_generic_bound_project`.
- `adr106_no_ir_exec_pbc_storage_wendelon_information_schema_surface`.
- Retarget `adr100_derived_table_source_is_narrow_not_general_subquery_support` (D9).

## Council history

Iter-0 menu: Claude led aggregate-derived (narrow) with SQL CASE as the bigger alt; Gemini led derived
aggregates/**sort/limit** (broad, `lower_query` delegation); Codex led `information_schema.schemata`. Iter-1:
Claude conceded his own iter-0 "schemata is inert" error (non-default namespaces *are* user-creatable via
two-part `ObjectName`s → `TableRef::with_schema`, `sql_planner.rs:597`; credit Codex's namespace grounding),
then leaned narrow within derived. Codex withdrew schemata, joined derived-aggregate, and **grounded the
decisive scope correction**: don't delegate to `lower_query` (it bypasses `validate_derived_select_shape`,
re-admitting nested-derived/joins-inside/EXISTS). Operator locked aggregate derived tables. Iters 2–3: all
three duals converged on the narrow fence-lift; **Codex caught the ADR 100 architecture-guard retarget (D9)**
that the others had only applied to the behavior test. Genuine 3-of-3. `information_schema.schemata`,
sort/limit-in-derived, joins-around-derived, and SQL CASE are carried follow-ons.

## What to do next

Sort/limit-in-derived (pairs with Rune sort/limit); joins-around-derived (needs qualifier + join-planning
proof); SQL CASE (substrate cut — new `Expr::Case` + executor kernel); `information_schema.schemata` (small
102 continuation, now confirmed non-inert); general project-fusion only on a concrete post-105 plan-shape
defect.

## Promotion criteria

Grouped and global aggregate-derived tables bind and execute through the existing aggregate substrate; alias
column lists rename aggregate outputs via the generic 104 path (105 coalesces redundancy); inner HAVING
resolves against the inner catalog; aggregate outputs stay non-transparent for elision; the general
derived-source fences (nested, joins-inside/around, ORDER BY/LIMIT, CTE, set-ops, windows, DISTINCT) remain;
ADR 100 guard + the source-scope behavior test reconciled; no IR/binder/executor/PBC/storage/Wendelon/
`information_schema` surface change; Toron suite green. Migration:
`plans/impl/106_derived_aggregate_tables.md`.
